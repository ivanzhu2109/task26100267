# desktoppool — 桌面池批量供应协调器

以固定容量 500 台的桌面池为对象，协调批量供应（每批 1–50 台）从发起到
Ready 之前的生命周期。不实现真正的虚拟机启动，也不要求进程重启恢复。

## 组成

- `executor.go` — `Executor` 接口与不透明句柄 `Handle`。
- `local_executor.go` — 可运行的进程内执行器 `LocalExecutor`。
  - `Create` 成功返回全局唯一句柄；返回错误保证无遗留资源。
  - `Create` 先登记资源再延迟返回，可模拟“忽略取消、晚到成功”。
  - `Delete` 可暂时失败、重复删除幂等，只有成功才保证资源不存在。
  - `List` 枚举当前存活资源。
  - `SetCreateHook` / `SetDeleteHook` 提供测试同步点（阻塞/注入失败），
    钩子在内部锁之外执行，阻塞期间 `List` 仍可用。
- `coordinator.go` — 协调器 `Coordinator`，只通过 `Executor` 接口操作，
  不直接修改执行器内部资源集合。

## 使用

```go
exec := desktoppool.NewLocalExecutor()
c := desktoppool.NewCoordinator(exec)

// 整批接收并立即占用额度；容量不足整批拒绝（ErrInsufficientCapacity）。
id, err := c.StartBatch(50)

// 取消尚未 Ready 的批次；幂等。Accepted 不代表资源已清理完。
res, err := c.CancelBatch(id) // CancelAccepted / CancelAlreadyReady

// 池占用额度与各批次状态、已发布句柄的一致视图。
snap := c.Snapshot() // snap.Used, snap.Batches[i].State / .Handles
```

批次状态：`Provisioning → Ready`，或 `Provisioning/Undoing → Undoing →
Terminated`。Ready 批次一次性发布全部句柄并持续占用额度；撤销批次等全部
已发出的 Create 返回、全部已建资源删除完毕（失败自动重试）后才成为
Terminated 并一次性释放整批额度。

并发保证：同批 Create 并发发出（可同时多个未返回）；协调器持锁时从不调用
执行器，`StartBatch` / `CancelBatch` / `Snapshot` 不等待任何 Create/Delete
结束；一个批次的创建或清理阻塞不影响其他批次推进。

## 测试

```sh
go test -race ./...
```

测试用可控同步点（gate + 钩子）固定交错，不依赖 sleep 碰运气，并同时核对
协调器快照与执行器真实存活资源：

- `scenario1_test.go` — 450 台 Ready 后接收 50 台；最后一个 Create 已产生
  资源但未返回时取消；删除暂停期间新批次因容量不足被拒；放行晚到成功、删除
  暂时失败后自动重试，最终只剩原 450 台且额度恰好释放一次。
- `scenario2_test.go` — 最后一次创建完成与取消竞争：创建胜出（Ready 后取消
  返回 AlreadyReady）与取消胜出（接受后永不发布、整批撤销）两种交错，穿插
  重复取消；快照全程无“接受取消后发布”或部分发布。
- `scenario3_test.go` — 撤销批次的 Delete 被阻塞时，有足够额度的另一批次
  仍能接收并成为 Ready，查询与重复取消正常完成。
