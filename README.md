# closer

Lightweight shutdown task runner for Go services.

[Read this in Russian](./README.ru.md)

## How it works

`closer` stores named shutdown tasks and executes them in reverse order (LIFO) when `Close()` is called.

- `Add(name, fn)` registers a task.
- `Close()` runs all tasks and logs results with `slog`.
- `TotalTimeout` limits the full shutdown process.
- `FuncTimeout` limits each individual task.
- Panics inside a task are recovered and logged as errors.

If no timeout is set (`0`), it is treated as unlimited.

## Usage

```go
package main

import (
	"context"
	"log/slog"
	"os"
	"time"

	"github.com/wk/pkg/closer"
)

func main() {
	log := slog.New(slog.NewTextHandler(os.Stdout, nil))

	c := closer.New(log, &closer.Config{
		TotalTimeout: 10 * time.Second,
		FuncTimeout:  3 * time.Second,
	})

	c.Add("db", func(ctx context.Context) error {
		// close database connection here
		return nil
	})

	c.Add("http-server", func(ctx context.Context) error {
		// shutdown HTTP server here
		return nil
	})

	c.Close()
}
```
