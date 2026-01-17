package closer

import (
	"context"
	"errors"
	"log/slog"
	"slices"
	"sync"
	"time"
)

var (
	errGlobalTimeoutExceeded = errors.New("global timeout exceeded")
	errFuncTimeoutExceeded   = errors.New("task timeout exceeded")
)

type Closer interface {
	// Add task to close
	Add(name string, close func(ctx context.Context) error)
	// Close start to close tasks
	Close()
}

// Config for closer
type Config struct {
	TotalTimeout time.Duration `env:"CLOSER_TOTAL_TIMEOUT"`
	FuncTimeout  time.Duration `env:"CLOSER_FUNC_TIMEOUT"`
}

type task struct {
	name  string
	close func(ctx context.Context) error
}

type tasks []task

type closer struct {
	closeErrsCount    int
	closeSuccessCount int
	tasks             tasks
	mu                sync.Mutex
	log               *slog.Logger
	cfg               *Config
}

// New create or returns instance of closer (singleton) and start os.Interrupt signal monitoring
//
// default config:
//   - totalDuration: infinity
//   - funcDuration: infinity
func New(log *slog.Logger, cfg *Config) Closer {
	if cfg.TotalTimeout < 0 {
		cfg.TotalTimeout = 0
	}
	if cfg.FuncTimeout < 0 {
		cfg.FuncTimeout = 0
	}

	return &closer{
		log: log,
		cfg: cfg,
	}
}

// Add adding a task to close
func (c *closer) Add(name string, close func(ctx context.Context) error) {
	c.mu.Lock()
	c.tasks = append(c.tasks, task{
		name:  name,
		close: close,
	})
	c.mu.Unlock()
}

// Close tasks from Tasks.
// Runtime errors are logged using the supplied logger.
func (c *closer) Close() {
	c.mu.Lock()
	tasks := slices.Clone(c.tasks)
	c.mu.Unlock()

	if len(tasks) == 0 {
		c.log.LogAttrs(nil, slog.LevelInfo, "No tasks to close")
		return
	}

	var (
		ctx    = context.Background()
		cancel context.CancelFunc
	)

	if c.cfg.TotalTimeout != 0 {
		ctx, cancel = context.WithTimeout(ctx, c.cfg.TotalTimeout)
		defer cancel()
	}

	c.log.LogAttrs(nil, slog.LevelInfo, "Closer is starting to close tasks", slog.Int("task_count", len(tasks)))

	timer := time.Now()
	for i := len(tasks) - 1; i >= 0; i-- {
		if ctx.Err() != nil { // check global timeout
			c.failureLog(ctx, i)
			return
		}

		task := tasks[i]
		if err := c.doTask(ctx, task); err != nil {
			if errors.Is(err, errGlobalTimeoutExceeded) { // if the global timeout expired while executing the task
				c.log.LogAttrs(nil, slog.LevelError, "Unfinished task", slog.String("task_name", task.name), slog.String("error", err.Error()))
				c.failureLog(ctx, i-1)
				return
			}

			c.closeErrsCount++

			if errors.Is(err, errFuncTimeoutExceeded) {
				c.log.LogAttrs(nil, slog.LevelError, "Unfinished task", slog.String("task_name", task.name), slog.String("error", err.Error()))
				continue
			}

			c.log.LogAttrs(nil, slog.LevelError, "Task error", slog.String("task_name", task.name), slog.String("error", err.Error()))
			continue
		}

		c.closeSuccessCount++
		c.log.LogAttrs(nil, slog.LevelInfo, "Task complete", slog.String("task_name", task.name))
	}

	c.successLog(timer)
}

// doTask executes the transferred task, while monitoring the global and local context.
func (c *closer) doTask(globalCtx context.Context, task task) (err error) {
	done := make(chan error, 1)

	var fnCtx = globalCtx
	var cancel context.CancelFunc

	if c.cfg.FuncTimeout != 0 {
		fnCtx, cancel = context.WithTimeout(globalCtx, c.cfg.FuncTimeout)
		defer cancel()
	}

	go func() {
		defer func() {
			if p := recover(); p != nil {
				c.log.LogAttrs(nil, slog.LevelError, "closer.doTask() - panic when closing a task")
				done <- errors.New("panic")
				return
			}
		}()
		done <- task.close(fnCtx)
	}()

	select {
	case <-fnCtx.Done():
		if globalCtx.Err() != nil {
			return errGlobalTimeoutExceeded
		}
		return errFuncTimeoutExceeded
	case err := <-done:
		return err
	}
}

// successLog
func (c *closer) successLog(timer time.Time) {
	duration := time.Since(timer)

	errCount := c.closeErrsCount

	if errCount > 0 {
		c.log.LogAttrs(nil, slog.LevelError, "Closer finished with errors",
			slog.Duration("execution_time", duration),
			slog.Int("failed_tasks_count", errCount),
		)
	} else {
		c.log.LogAttrs(nil, slog.LevelInfo, "Closer finished, all tasks closed",
			slog.Duration("execution_time", duration),
		)
	}
}

// failureLog
func (c *closer) failureLog(ctx context.Context, lastIdx int) {
	tasks := c.tasks
	successCloseCount := c.closeSuccessCount

	for i := lastIdx; i >= 0; i-- {
		c.log.LogAttrs(nil, slog.LevelError, "Unprocessed task", slog.String("task_name", tasks[i].name), slog.String("error", errGlobalTimeoutExceeded.Error()))
	}

	c.log.LogAttrs(nil, slog.LevelError,
		"Global timeout exceeded, closer stop",
		slog.Duration("time_limit", c.cfg.TotalTimeout),
		slog.Int("success_task_count", successCloseCount),
		slog.Int("failure_task_count", len(tasks)-successCloseCount),
		slog.String("error", ctx.Err().Error()),
	)
}
