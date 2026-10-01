package desktoppool

import "context"

// Handle 是实际桌面的不透明句柄。
type Handle struct{ id uint64 }

// Executor 是进程内资源执行器接口。
type Executor interface {
	Create(ctx context.Context) (Handle, error)
	Delete(ctx context.Context, h Handle) error
	List() []Handle
}
