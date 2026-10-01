package desktoppool

import (
	"context"
	"sort"
	"sync"
)

// LocalExecutor 是可运行的进程内资源执行器。
//
// 行为约定：
//   - Create 成功返回全局唯一句柄；返回错误时保证没有遗留资源。
//   - Create 会先把资源登记为存活，再长时间延迟返回（模拟真实创建）；
//     入口时若 ctx 已取消则直接失败，延迟期间是否响应取消由 CreateHook 决定，
//     因此可以模拟“忽略取消信号而晚到成功”。
//   - Delete 可以暂时失败；重复删除幂等；只有成功才保证资源不存在。
//
// CreateHook / DeleteHook 是测试与演示用的同步点，在资源已登记（Create）
// 或删除生效前（Delete）调用，可以阻塞或返回错误。钩子不在内部锁内执行，
// 因此阻塞期间 List 等操作仍可工作。协调器只通过 Executor 接口访问本类型，
// 不会直接修改内部资源集合。
type LocalExecutor struct {
	mu   sync.Mutex
	live map[uint64]struct{}
	next uint64

	createHook func(ctx context.Context, h Handle) error
	deleteHook func(ctx context.Context, h Handle) error
}

func NewLocalExecutor() *LocalExecutor {
	return &LocalExecutor{live: make(map[uint64]struct{})}
}

// SetCreateHook 设置/替换 Create 钩子（并发安全）。
func (e *LocalExecutor) SetCreateHook(hook func(ctx context.Context, h Handle) error) {
	e.mu.Lock()
	e.createHook = hook
	e.mu.Unlock()
}

// SetDeleteHook 设置/替换 Delete 钩子（并发安全）。
func (e *LocalExecutor) SetDeleteHook(hook func(ctx context.Context, h Handle) error) {
	e.mu.Lock()
	e.deleteHook = hook
	e.mu.Unlock()
}

func (e *LocalExecutor) Create(ctx context.Context) (Handle, error) {
	if err := ctx.Err(); err != nil {
		return Handle{}, err
	}

	e.mu.Lock()
	e.next++
	h := Handle{id: e.next}
	e.live[h.id] = struct{}{}
	hook := e.createHook
	e.mu.Unlock()

	if hook != nil {
		if err := hook(ctx, h); err != nil {
			e.mu.Lock()
			delete(e.live, h.id)
			e.mu.Unlock()
			return Handle{}, err
		}
	}
	return h, nil
}

func (e *LocalExecutor) Delete(ctx context.Context, h Handle) error {
	e.mu.Lock()
	_, exists := e.live[h.id]
	hook := e.deleteHook
	e.mu.Unlock()

	if !exists {
		return nil // 幂等：资源本就不存在
	}
	if hook != nil {
		if err := hook(ctx, h); err != nil {
			return err // 暂时失败：资源仍然存活
		}
	}

	e.mu.Lock()
	delete(e.live, h.id)
	e.mu.Unlock()
	return nil
}

// List 枚举当前存活资源（按句柄 ID 排序）。
func (e *LocalExecutor) List() []Handle {
	e.mu.Lock()
	defer e.mu.Unlock()
	out := make([]Handle, 0, len(e.live))
	for id := range e.live {
		out = append(out, Handle{id: id})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].id < out[j].id })
	return out
}
