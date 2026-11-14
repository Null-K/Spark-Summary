# Spark Summary

把 Minecraft [spark](https://spark.lucko.me/) 性能报告压缩成 **AI 可读的 Markdown 摘要**，用于让 ChatGPT / Claude / DeepSeek 等大模型分析服务器卡顿原因。

一份 spark 报告的原始 JSON 动辄数 MB（约 100 万+ tokens），直接喂给 AI 既贵又容易超出上下文。本工具在本地完成调用树聚合、插件归因、实体统计、配置抽取和规则诊断，输出约 **8k tokens** 的摘要，保留分析卡顿所需的关键信息。

```
原始 JSON 4198KB → 摘要 31.0KB，约 8192 tokens
```

## 特性

- **一条命令**：直接粘贴玩家发来的 `https://spark.lucko.me/xxxx` 链接即可，自动下载完整数据并缓存
- **零依赖**：仅使用 Python 标准库，单文件脚本
- **自动诊断**：基于阈值规则标出 `[严重]` / `[警告]` / `[提示]`，如 MSPT 超载、Swap 使用、内存超配、GC 停顿过长、实体堆积、插件占用过高等
- **调用树压缩**：
  - 按插件/模组归因 CPU 时间（插件调用的服务端代码计入该插件）
  - 每个插件的入口方法与热点方法
  - 自身耗时最高的方法 Top 40
  - 裁剪 + 单子链折叠后的热点调用路径
- **历史时间线**：采样前约 1 小时的 TPS / MSPT / CPU / 内存 / 玩家 / 实体 / 区块 / 延迟，按 5 分钟聚合成表格
- **实体分析**：实体类型排行、各世界明细、最密集区块、最密集 128×128 区域（坐标可直接 `/tp`）
- **配置抽取**：从 server.properties、bukkit、spigot、paper、purpur、leaf、gale 等配置中只提取与性能相关的项
- **环境信息**：服务端版本、硬件、JVM 参数、内存池、GC 统计、插件列表（含简介）

## 环境要求

- Python 3.8+（在 3.13 上测试）
- 可选：`pip install tiktoken`，用于精确统计摘要 token 数（未安装时为粗略估算）

## 使用

```bash
# 直接使用 spark 链接
python spark_summary.py https://spark.lucko.me/0Nl8r2TPYn

# 也可以只输入报告 ID，或本地 JSON 文件
python spark_summary.py 0Nl8r2TPYn
python spark_summary.py ./report.json

# 不带参数时会交互式提示输入链接
python spark_summary.py

# 网络无法直连时使用代理
python spark_summary.py https://spark.lucko.me/0Nl8r2TPYn --proxy http://127.0.0.1:7897
```

摘要会输出到终端，同时保存为 `summary_<ID>.md`。下载的原始报告缓存在脚本同目录的 `cache/` 下，再次分析同一报告无需重新下载。

### 参数

| 参数 | 说明 | 默认值 |
|---|---|---|
| `source` | spark 链接 / 报告 ID / 本地 JSON 文件 | 交互输入 |
| `-o, --output` | 输出 Markdown 路径 | `summary_<ID>.md` |
| `--proxy` | HTTP 代理地址 | 无（也会读取系统 `HTTPS_PROXY` 环境变量） |
| `--no-cache` | 忽略缓存重新下载 | 关闭 |
| `--min-pct` | 热点调用路径的最小占比（%），越小越详细 | `0.5` |
| `--max-lines` | 热点调用路径最多行数 | `150` |

想要更精简的摘要（约 4k tokens），可以使用 `--min-pct 1 --max-lines 60`。

## 摘要结构

```
# spark 报告摘要
## 自动诊断          规则诊断结果，按严重程度排序
## 概览              平台/版本、采样参数、运行时长、系统/CPU/Java 参数
## 性能指标          TPS、MSPT、时间窗口、CPU、堆内存与内存池、物理内存/Swap、GC、延迟、磁盘
## 历史时间线        5 分钟粒度的历史指标表格
## 线程 CPU 时间分布  按插件、插件明细、自身耗时 Top 方法、热点调用路径
## 实体与区块        实体排行、各世界明细、密集区块/区域、非默认游戏规则、数据包
## 关键性能配置      各配置文件中与性能相关的项
## 插件/模组列表     名称、版本、简介
```

<details>
<summary>输出示例（节选）</summary>

```markdown
## 自动诊断
- [严重] MSPT 中位数 52.4ms > 50ms，主线程持续超载
- [严重] 系统 Swap 已使用 20.6GB，JVM 内存被换出到磁盘会导致严重卡顿和 GC 变慢
- [严重] 最大堆 20.0GB + 非堆 0.5GB 接近/超过物理内存 24.0GB，内存超配
- [警告] G1 Young Generation 平均停顿 94ms 偏高（理想 <50ms）
- [警告] 掉落物 1401 个，偏多

### 按插件/模组（含其调用的服务端代码）
- 94.8% (服务端/JDK)
- 2.0% SpeedReport
- 1.7% ProtocolLib

### 热点调用路径（≥0.5%，单子链已折叠，A→B 表示链首/链尾占比）
- 100.0% Thread.run → … → MinecraftServer.processPacketsAndTick
  - 95.8% DedicatedServer.tickServer → MinecraftServer.tickServer → MinecraftServer.tickChildren
    - 85.0% MinecraftServer.tickLevel → ServerLevel.tick
      - 49.8% EntityTickList.forEach → … → ServerLevel.lambda$tick$0
      - 19.5% ServerChunkCache.tick
```

</details>

## 交给 AI 分析

1. 运行脚本生成 `summary_<ID>.md`
2. 将 [提示词.md](提示词.md) 中的提示词作为系统提示词（或放在消息开头）
3. 把摘要内容粘贴在提示词之后发送给 AI

提示词中包含摘要各字段的阅读说明（如百分比含义、调用链折叠记号、Windows Swap 统计的偏差等），能显著减少 AI 的误读，并约束其输出带证据、可执行的优化方案。

## 局限

- 目前完整支持 **CPU 采样报告**（`/spark profiler`）；堆内存摘要等其他类型只输出基础信息
- 自动诊断阈值是经验值，仅作参考线索，最终结论应结合整份摘要判断
- Windows 上 spark 报告的 Swap 数值可能包含已提交内存，可能偏高
- 采样时间过短（< 2 分钟）时结果可能不具代表性，建议使用 `/spark profiler start --timeout 300` 采样 5 分钟以上
- 摘要中包含服务器配置与插件列表，分享前请自行确认无敏感信息

## 致谢

- [spark](https://github.com/lucko/spark) by lucko
