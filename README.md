# CANopen Node Emulator (Go)

一个完全在进程内运行、**不依赖真实 CAN 总线硬件或任何外部服务**的多节点
CANopen 通信与故障恢复模拟器，用于在并发与故障注入下验证：

- NMT 状态管理与心跳监控的结论稳定性；
- SDO 对对象字典（OD）的读写（含分段传输、中断与重试）；
- PDO 映射、周期收发以及总线错误/节点离线后的恢复；
- 异常帧、重复帧、乱序帧、超时帧不会造成错误状态扩散。

所有时序既可用真实时钟（`RealClock`）运行，也可用确定性的假时钟
（`FakeClock`，带可重复触发的 ticker）驱动单测，因此「同一输入 → 同一结论」。

## 包结构（根包 `canopen`）

| 文件 | 职责 |
| --- | --- |
| `clock.go` | `Clock` 抽象、`RealClock`、确定性 `FakeClock`（one-shot + 周期 ticker，丢拍合并保序） |
| `bus.go` | 软件 CAN 总线 `VirtualBus`：硬件风格掩码过滤、发送者自回环排除、六类故障注入、原子计数 |
| `od.go` | 并发安全对象字典 `ObjectDict`：类型/长度/权限校验、读后副本、写时原子替换 |
| `nmt.go` | NMT 状态机、NMT 主站命令、心跳生产者、心跳监控（超时/恢复闩锁） |
| `sdo.go` | SDO 服务器与客户端：expedited + segmented 上传/下载、abort 码、超时与重试 |
| `pdo.go` | PDO 管理器：映射校验、周期 TPDO、RPDO 接收/去重/陈旧判定、OD 原子写入 |
| `node.go` | `Node`：聚合 NMT + 心跳 + SDO + PDO，状态切换联动 |
| `network.go` | `Network`：拥有总线与节点集，承担 NMT 主站、监控工厂、故障注入/恢复 |
| `log.go` | goroutine 安全的分级日志，打印关键帧、状态变化与判定依据 |

## 支持的协议范围

### NMT（CiA-301 从站状态机）

状态：`INITIALISING(0)`、`STOPPED(4)`、`OPERATIONAL(5)`、`PRE-OPERATIONAL(127)`。

合法迁移（非法迁移被拒绝并保持原态，不会回退）：

```
INITIALISING -> PRE-OPERATIONAL
PRE-OPERATIONAL <-> OPERATIONAL
PRE-OPERATIONAL -> STOPPED
OPERATIONAL <-> STOPPED
RESET-NODE / RESET-COMMUNICATION: * -> INITIALISING -> PRE-OPERATIONAL
```

主站命令经 COB `0x000`（2 字节：命令 + 节点号）发送；节点号 `0` 为广播。
节点启动时发送一次 boot-up 帧（COB `0x700+id`，数据 `0x00`），随后按配置
周期发送心跳帧（数据字节为当前 NMT 状态）。

### 心跳监控

- 监控器订阅 COB `0x700+id`，超过 `timeout` 未收到心跳即判定超时。
- **超时只闩锁一次**；超时不会因继续无心跳而重复上报。
- **重复/迟到的心跳永远不会把节点带入错误状态**：超时后再次收到心跳，
  上报的是 `RECOVERED`（并恢复 alive），而不是错误。
- 心跳生产者在唤醒时按「经过时间」补发缺失周期，调度延迟或假时钟合并
  唤醒都不会静默丢拍。

### SDO（默认通道，COB `0x600+id` / `0x580+id`）

- 客户端上传/下载：≤4 字节走 expedited，更长走 segmented（每段 7 字节，
  toggle bit 交替）。
- 失败返回可区分的协议级原因（`SDOAbortError.Code`，CiA-301 abort 码）：

  | 场景 | abort 码 |
  | --- | --- |
  | 索引/子索引不存在 | `0x06020000` / `0x06090011` |
  | 写只读对象 | `0x06010002` |
  | 读只写对象 | `0x06010001` |
  | 类型/长度不匹配 | `0x06070010`（过长 `…012` / 过短 `…013`） |
  | toggle bit 未交替 | `0x05030000` |
  | 非法命令字 | `0x05040001` |
  | 本地超时（可重试，非线上 abort） | `ErrSDOTimeout`（`errors.Is` 判定） |

- **失败传输不留下部分写入**：分段下载只在最后一段校验通过后由 OD 一次性
  原子提交；中途 abort/超时则丢弃全部缓冲，原值保持不变。
- 客户端区分「可重试的超时」与「确定的线上 abort」：前者按 `MaxRetries`
  重试（每次都是全新 initiate，不携带半截状态），后者立即返回。
- 遵循单默认通道规则：同一服务器同一时刻只处理一个传输，交错的分段帧会
  得到明确的 toggle/sequence abort，而不是损坏数据。

### PDO

- 映射：每个 TPDO/RPDO 由若干 `(index, sub, bits)` 组成，总长超过 8 字节
  返回 `ErrPDOLength`，无映射返回 `ErrNoPDOMapping`。
- TPDO 周期性发送（默认 COB `0x180+n*0x100+id`），支持 `Sync()` 单发；
  唤醒合并时按经过时间补发，周期不漂移。
- RPDO（默认 `0x200+n*0x100+id`，可显式指定 COB-ID）仅在 `OPERATIONAL`
  生效；每个映射对 OD 的写入相互独立且原子。
- **异常帧不扩散错误**：
  - 短帧/长度不足 → 计入 `RxOverrun`，忽略，不进错误态；
  - 非 OPERATIONAL → 计入 `RxIgnored`，忽略；
  - 可选序号字节（`RPDOConfig.EnableSeq/SeqByte`）：相同序号 → `RxDuplicate`
    幂等接受；序号倒退 → `RxStale` 忽略，旧数据不覆盖新数据；
  - **未启用序号时**，每一帧都幂等应用——即使映射数据恒定（慢变化过程
    数据的正常现象），周期帧也不会被误判为重复而丢弃。
- 恢复后的数据可解释：乱序到达时仅采用「不早于已应用序号」的帧，最终 OD
  反映最新有效值；重复写入相同值天然幂等。

## 故障注入（`VirtualBus.Inject(FaultSpec)`）

| 故障 | 行为 |
| --- | --- |
| `FaultDrop` | 帧被丢弃（计入 `Dropped`） |
| `FaultDelay` | 帧延迟 `Delay` 后送达（迟到帧） |
| `FaultDuplicate` | 帧背靠背送达两次 |
| `FaultReorder` | 帧被延迟，制造乱序 |
| `FaultCorrupt` | 翻转首字节 LSB（计入 `Corrupted`） |
| `FaultBusOff` | 总线进入 bus-off，所有帧被丢弃，直到 `Recover()` |

`FaultSpec.Targets` 为节点号列表时只作用于该节点产生的 COB-ID；为空作用于
全部。`Probability` 控制命中概率。`Network.Recover()` / `bus.Recover()` 清除
故障并退出 bus-off；周期生产者全程继续循环，恢复后无需手动复位即可继续。

## 并发模型与安全

- 总线投递在发布者 goroutine 内同步、按订阅顺序进行；订阅/取消订阅、故障
  状态、统计计数全部加锁或用原子操作。
- 帧不会回环给发送者（标准 CAN 控制器行为）；订阅用
  `(id & mask) == (filter & mask)` 硬件风格过滤。
- NMT 机、OD、SDO 会话、PDO 统计均由互斥保护；状态历史只追加，迁移串行化，
  并发命令下不会出现状态回退或脏历史。
- 单测全部通过 `-race`。

## 本地验证

```bash
# 构建
go build ./...

# 运行全部单测（确定性假时钟 + 竞态检测）
go test -race ./...

# 多轮重复，验证结论稳定、无偶发
go test -race -count=30 ./...

# 运行可执行演示（真实时钟，无需硬件）
go run ./cmd/demo
```

单测在传入 `Logger` 时会打印关键帧（`FRM`：COB-ID/DLC/数据/方向）、状态
变化（`NMT ... -> ...`）以及每条判定依据（接受/去重/陈旧/短帧/超时/恢复/
abort 原因）。`TestLoggingPrintsFramesStatesAndReasons` 即对这些日志内容做
断言。

## 主要测试场景

- `TestNMT*`：合法/非法迁移、重置、并发迁移下无回退、历史只追加。
- `TestHeartbeat*`：超时一次性、迟到心跳触发恢复、重复心跳不报错。
- `TestSDO*`：expedited/分段往返；不存在/只读/只写/长度不符的可区分 abort；
  分段失败不留部分写入；超时、丢帧后重试成功；传输中断。
- `TestPDO*`：映射校验、周期收发、重复/乱序/短帧隔离、非 OPERATIONAL 忽略、
  恒定数据周期帧全部接受、bus-off 后自动恢复。
- `TestConcurrent*` / `TestNodeOfflineAndRecovery`：多节点并发心跳、节点离线
  超时与同号节点重新上线恢复、并发 SDO + 背景总线流量下零竞态。
