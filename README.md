# paged-kv-cache-go

面向本地推理运行时的**分页 KV 缓存内存管理器**。它把原本整段连续保存的键值状态
切分为固定大小的块（block / page），提供块级共享、多租户隔离、内存不足时的抢占
与换出，以及服务重启后的状态恢复。

- 纯 Go，无外部依赖；并发安全（内部用一把元数据锁 + 不可变块负载，读取无半写）。
- 所有持久化数据均带校验和（CRC32 + 每记录 CRC），损坏/半写数据绝不作为有效缓存返回。

## 安装

```go
import "github.com/guoyyit/paged-kv-cache-go/pkg/pagedkv"
```

## 快速开始

```go
m, err := pagedkv.New(pagedkv.Config{
    BlockBytes:         16 * 1024, // 每个块固定 16 KiB
    TotalBlocks:        4096,      // 全局物理块池大小
    DataDir:            "./kvdata",// 持久化目录；为空则仅内存（不落盘）
    DefaultTenantQuota: 1024,      // 新租户默认驻留块配额
    Policy:             pagedkv.EvictLRU,
    SyncWAL:            true,      // 每条提交记录 fsync
    MaxSwapBytes:       1 << 30,   // 换出+缓存的磁盘上限（0=不限）
})
if err != nil { log.Fatal(err) }
defer m.Close()

if err := m.CreateTenant("team-a", pagedkv.TenantConfig{QuotaBlocks: 512}); err != nil {
    log.Fatal(err)
}

seq, err := m.NewSequence("team-a", "req-123")
if err != nil { log.Fatal(err) }

seq.Append(kvChunk0)           // 追加写，自动按 BlockBytes 切块
seq.Append(kvChunk1)
seq.Rewrite(offset, patched)   // 改写共享前缀时自动 copy-on-write
buf, _ := seq.Read(0, n)       // 读取返回独立副本
seq.Commit()                   // 发布为可共享前缀（持久化）
// 或 seq.Abort()              // 丢弃，未发布页立即归还
```

内存压力下，活动序列会被换出；之后可恢复：

```go
if _, err := seq.Read(0, n); errors.Is(err, pagedkv.ErrSequencePreempted) {
    if err := seq.Resume(); errors.Is(err, pagedkv.ErrNeedsRecompute) {
        // 换出镜像缺失或损坏：必须从前缀重算，管理器绝不会返回半截内容
        recompute()
    }
}
```

## 分页语义

- **固定块大小**：`BlockBytes` 为每个物理块的负载字节数。逻辑缓冲按顺序切为整数块，
  最后一块不足部分以零填充；块内容哈希覆盖完整块（含填充）。逻辑长度单独记录，读取
  永远不会越过逻辑末尾。
- **内容寻址与块共享**：每个块的身份是其负载的 SHA-256 哈希。**同一租户内**，内容完全
  相同的块只保留一份物理副本，多个请求/序列的槽位指向同一物理块（引用计数）。
- **写时复制（COW）**：被引用的块负载不可变。任一请求对共享块执行 `Rewrite`（或向共享的
  末块 `Append`）时，只复制受影响的块，其他序列继续看到旧内容；未触及的后续块仍共享。
- **块生命周期**：
  - `active`：被活动序列槽位引用（`refs > 0`），不可被回收。
  - `cached`：已发布（`Commit`/换出）且当前无引用，作为可复用前缀保留，可在 LRU 回收时
    归还全局池。
  - 已换出/已提交的内容由内容存储（`objects/`）持有，物理页被回收后可按哈希惰性重新载入。

## 多租户隔离与配额

- 每个租户拥有独立的：驻留块表、内容哈希去重索引、已提交前缀树、换出镜像表。
- **回收与抢占只在发起请求的租户内部发生**：租户 A 分配内存时，只会回收/抢占租户 A 自己
  的缓存页或活动序列，绝不动租户 B 的活动块，也无法读到 B 的已提交前缀。
- 当全局池被其他租户占满、本租户又无自身可回收内存时，分配以
  `ErrTenantQuotaExceeded`（命中本租户配额）或 `ErrResourceExhausted`（全局池被他人占用）
  明确失败，而不是挤占他人。
- 跨租户访问序列表现为 `ErrSequenceNotFound`；未知租户为 `ErrTenantNotFound`；越界为
  `ErrOutOfRange`，便于调用方区分失败原因。

### 配额参数

| 参数 | 含义 |
| --- | --- |
| `TotalBlocks` | 全局物理块池大小（硬上限，所有租户驻留块之和不超过它）。 |
| `DefaultTenantQuota` | 创建租户时未显式指定配额时使用的默认驻留块数（上限为 `TotalBlocks`）。 |
| `TenantConfig.QuotaBlocks` | 单个租户的驻留块上限（活动 + 缓存）。 |

## 淘汰与抢占

回收顺序**确定性**：相同输入永远做出相同决策。

1. 内容去重：租户内已有相同块则直接共享，不分配新页。
2. 全局空闲页 + 租户仍有配额余量 → 取一个空闲页（空闲 id 升序）。
3. 否则按 **LRU**（逻辑访问时钟 `(atime, block_id)` 升序）回收该租户自身的、不属于"正在
   增长序列自身链"的缓存页。
4. 仍不足则**抢占**：选择该租户中换出后能释放最多独占物理页的活动序列（并列时取最小
   序列 id），先把其全部块写入内容存储（write-ahead），再追加 `swap` 日志并释放引用。
- 被抢占序列进入 `swapped` 状态，读返回 `ErrSequencePreempted`；`Resume()` 逐块从内容
  存储载入（内容相同的驻留页继续复用），并重新校验校验和。
- 若任一镜像对象缺失或校验失败，序列被永久标记为 `needs_recompute`，返回
  `ErrNeedsRecompute`，且不会暴露任何不完整内容。

### 淘汰策略参数

| 值 | 策略 |
| --- | --- |
| `EvictLRU`（默认，零值同此） | 最少访问优先，`block_id` 作为确定性平局裁决。 |

### 碎片控制

- 块定长 + 全局空闲页栈，天然无外部碎片。
- **未发布**（未提交、未换出）的内容在引用归零/中止时立即归还物理池，不残留。
- 已发布内容仅作为可随时回收的 `cached` 页；在长序列交替、提交/中止混合负载下，驻留集
  始终被 `TotalBlocks` 封顶，且全部可回收（见 `TestNoPersistentFragmentation`）。

## 持久化与重启恢复

`DataDir` 布局：

```
DataDir/
  journal.log        # 追加式 WAL，每帧: magic | len | crc32 | JSON(record)
  objects/<ab>/<sha256>   # 不可变块对象，文件尾附 uint32 长度 + uint32 CRC32
```

- **WAL 记录**：建租户、提交序列（块哈希链 + 长度）、换出、恢复、中止（墓碑）。
- **写入顺序**：提交/换出时先把块对象落盘（临时文件 + 原子 rename + fsync），成功后才写
  WAL，保证"映射"与"换出数据"对齐。
- **重启恢复**：
  - 打开日志时扫描帧，遇到损坏/半写尾部直接截断并计入 `Stats.CorruptRecords`，**不阻塞启动**。
  - 重放完整记录；对每条块哈希链**实际读取并校验**每个对象。链上任一块缺失/损坏，整条
    序列标记为 `needs_recompute`，绝不插入前缀缓存、绝不作为有效缓存返回。
  - 块池冷启动为空，已提交/已换出内容在首次访问时按哈希惰性调页。
  - 恢复后对不再被任何持久序列引用的对象做垃圾回收。
- `SyncWAL=true` 时每条 WAL 记录 fsync；关闭管理器时会压缩日志并回收孤儿对象。

`MaxSwapBytes` 对 `objects/`（内存模式下为换出表）设上限，超限写入返回
`ErrResourceExhausted`，调用方可据此主动抢占/拒绝。

## 错误速查

| 错误 | 场景 |
| --- | --- |
| `ErrTenantNotFound` / `ErrSequenceNotFound` | 租户/序列不存在或跨租户访问 |
| `ErrOutOfRange` | 读写偏移越界 |
| `ErrTenantQuotaExceeded` | 租户配额用尽且内部无可回收内存 |
| `ErrResourceExhausted` | 全局池被占满 / 内容存储超限 |
| `ErrSequencePreempted` | 序列已被换出，需 `Resume()` |
| `ErrNeedsRecompute` | 镜像不可恢复，必须重算 |
| `ErrSequenceCommitted` / `ErrSequenceAborted` | 对已提交/已中止序列做非法变更 |
| `ErrCorrupt` | 对象或记录校验失败 |
| `ErrClosed` | 管理器已关闭 |

## 可观测性

`Stats()` 返回全局计数（总/驻留/活动引用/缓存/换出块数、租户数、损坏记录数、内容存储
字节数）；`TenantStats(id)` 返回单租户配额与驻留/活动/缓存/换出计数。

## 测试

```sh
go test -race ./...
```

覆盖：多租户配额/越界隔离、块共享与 COW 改写隔离、内存不足抢占与恢复、碎片控制与 LRU
回收确定性、重启恢复（提交/换出对齐、损坏对象标记重算、损坏日志尾部不阻塞启动）、高并发
分配/共享/改写的竞争安全，以及 owner 索引内部不变量。
