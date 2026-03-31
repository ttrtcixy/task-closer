# closer

Лёгкий раннер задач завершения для Go-сервисов.

## Как работает

`closer` сохраняет именованные задачи завершения и выполняет их в обратном порядке (LIFO) при вызове `Close()`.

- `Add(name, fn)` добавляет задачу.
- `Close()` запускает все задачи и пишет результат через `slog`.
- `TotalTimeout` ограничивает всё время завершения.
- `FuncTimeout` ограничивает время выполнения одной задачи.
- Паники внутри задачи перехватываются и логируются как ошибки.

Если таймаут равен `0`, ограничение не применяется.

## Пример использования

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
		// закрытие БД
		return nil
	})

	c.Add("http-server", func(ctx context.Context) error {
		// остановка HTTP-сервера
		return nil
	})

	c.Close()
}
```
