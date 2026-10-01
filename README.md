# desktoppool — 桌面池批量供应协调器

Go 实现的进程内桌面池批量供应协调器。池容量固定 500 台，每批 1–50 台，
支持并发提交与取消。只覆盖供应到 Ready 之前的生命周期：不启动真实虚拟机，
不做进程重启恢复。

## 组成

- `executor.go` — `Executor` 接口（不透明句柄 `Handle`、`Create`/`Delete`/`List`）
  与可直接运行的进程内执行器 `MemoryExecutor`。
- `controlled.go` — `ControlledExecutor`：包装 `MemoryExecutor`，通过
  `CreateHook`/`DeleteHook` 与一次性 `Gate` 提供可控同步点与失败注入，
  用于测试固定交错（长时间延迟、忽略取消的晚到成功、Delete 暂时失败）。
- `coordinator.go` — `Coordinator`：`StartBatch` / `CancelBatch` / `Snapshot`。

## 使用

```go
exec := desktoppool.NewMemoryExecutor()        // 或任何 Executor 实现
coord := desktoppool.NewCoordinator(exec, 500) // 容量上限 500

id, err := coord.StartBatch(50) // 立即占用 50 额度，异步供应
// err == ErrInsufficientCapacity 时整批拒绝，不占用额度

err = coord.CancelBatch(id)     // 未 Ready 则接受取消；重复调用幂等
// err == ErrBatchAlreadyReady 表示已 Ready 不可取消

snap := coord.Snapshot()        // 占用额度 + 各批次状态 + 已发布句柄的一致视图
```

批次状态机：`Creating → Ready`（全部 Create 成功返回后一次性发布全部桌面）
或 `Creating → Undoing → Terminated`（任一 Create 失败或取消被接受后整批撤销；
等所有已发出的 Create 返回、所有已创建资源删除完毕后，一次性释放整批额度）。
撤销期间与 Ready 期间均持续占用额度。

## 并发与保证

- 每个 Create 一个 goroutine：同一批次可多个 Create 同时在途，批次间互不阻塞。
- 协调器锁内不调用执行器；`StartBatch`/`CancelBatch`/`Snapshot` 不等待任何
  Create/Delete 结束。
- 取消只是信号：晚到成功的资源同样登记并删除；Delete 失败自动重试直到成功；
  额度只在 Terminated 时释放且仅释放一次。

## 测试

```sh
go test -race ./...
```

测试用 `Gate`/`Hook` 固定交错（不靠 sleep 碰运气），并同时核对协调器快照与
执行器真实存活资源：

- `TestCancelWithLateCreateAndTransientDeleteFailures`：450 台 Ready 后接收 50 台，
  最后一个 Create 已产生资源但未返回时取消；暂停删除期间新批次因容量不足被拒；
  放行晚到成功、Delete 经历暂时失败后，最终只剩原 450 台且额度恰好释放一次。
- `TestRaceLastCreateVsCancel`：最后一次创建完成与取消竞争，覆盖双方胜出及
  重复取消；快照观察者全程断言不出现接受取消后发布或部分发布。
- `TestUndoDoesNotBlockOtherBatches`：阻塞一个撤销批次的 Delete，另一个批次
  仍能接收并 Ready，查询与重复取消即时完成。
- `TestCreateFailureUndoesBatch` / `TestValidation`：Create 失败整批撤销、
  参数与状态校验。
