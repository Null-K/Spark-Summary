#!/usr/bin/env python3
"""将 spark 性能报告压缩为适合交给 LLM 分析的 Markdown 摘要。"""
import argparse
import gzip
import json
import os
import re
import sys
import urllib.error
import urllib.request
from collections import Counter, defaultdict
from datetime import datetime

HOST = "https://spark.lucko.me/"
CACHE_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "cache")
IDLE_METHODS = re.compile(r"^(park|parkNanos|sleep|wait|waitForTasks|managedBlock|waitUntilNextTick|poll|take)$")
CFG_KEYS = re.compile(
    r"view-distance|simulation-distance|entity-activation-range|tick-inactive|mob-spawn-range|spawn-limits|"
    r"ticks-per|merge-radius|despawn-rate|despawn-ranges|max-entity-collisions|hopper|redstone|"
    r"per-player-mob-spawns|entity-per-chunk-save-limit|max-auto-save-chunks|optimize-explosions|max-tick-time|"
    r"nerf-spawner|async|parallel|delay-chunk-unloads|tick-rates|io-threads|worker-threads|sync-chunk-writes|"
    r"network-compression-threshold|max-players|treasure-maps|max-tnt|entity-tracking-range|pause-when-empty"
)
CFG_MAX_PER_FILE = 80


# ---------- 加载 ----------

def parse_code(s):
    s = s.strip()
    m = re.search(r"spark\.lucko\.me/([A-Za-z0-9]+)", s)
    if m:
        return m.group(1)
    if re.fullmatch(r"[A-Za-z0-9]+", s):
        return s
    return None


def load_report(src, proxy=None, use_cache=True):
    if os.path.isfile(src):
        with open(src, encoding="utf-8") as f:
            return json.load(f), os.path.splitext(os.path.basename(src))[0]
    code = parse_code(src)
    if not code:
        raise SystemExit(f"无法识别的 spark 链接: {src}")
    path = os.path.join(CACHE_DIR, code + ".json")
    if use_cache and os.path.isfile(path):
        with open(path, encoding="utf-8") as f:
            return json.load(f), code
    req = urllib.request.Request(
        f"{HOST}{code}?raw=1&full=true",
        headers={"User-Agent": "spark-summary/1.0", "Accept-Encoding": "gzip"},
    )
    handlers = [urllib.request.ProxyHandler({"http": proxy, "https": proxy})] if proxy else []
    try:
        with urllib.request.build_opener(*handlers).open(req, timeout=120) as r:
            body = r.read()
            if r.headers.get("Content-Encoding") == "gzip":
                body = gzip.decompress(body)
    except urllib.error.HTTPError as e:
        raise SystemExit(f"下载失败: HTTP {e.code}（报告可能已过期或 ID 错误）")
    except urllib.error.URLError as e:
        raise SystemExit(f"下载失败: {e.reason}（可尝试 --proxy http://127.0.0.1:7897）")
    try:
        data = json.loads(body)
    except json.JSONDecodeError:
        raise SystemExit("返回内容不是 JSON，报告可能不存在")
    os.makedirs(CACHE_DIR, exist_ok=True)
    with open(path, "wb") as f:
        f.write(body)
    return data, code


# ---------- 工具函数 ----------

def gb(b):
    return f"{b / 1024 ** 3:.1f}GB"


def pct(x, total):
    return f"{x / total * 100:.1f}%" if total else "0%"


def f1(x):
    return f"{x:.1f}"


def stat_line(d):
    return f"均值 {f1(d['mean'])} / 中位 {f1(d['median'])} / p95 {f1(d['percentile95'])} / 最大 {f1(d['max'])}"


def tot(n):
    return sum(n["times"]) if "times" in n else n.get("time", 0)


def short(n):
    return n.get("className", "?").rsplit(".", 1)[-1] + "." + n.get("methodName", "?")


def short_key(key):
    cls, _, method = key.rpartition(".")
    return cls.rsplit(".", 1)[-1] + "." + method


# ---------- 调用树分析 ----------

class ThreadAnalyzer:
    def __init__(self, thread, report):
        self.thread = thread
        self.flat = thread.get("children", []) if "childrenRefs" in thread else None
        self.class_src = report.get("classSources") or {}
        self.method_src = report.get("methodSources") or {}
        self.line_src = report.get("lineSources") or {}
        self.total = tot(thread)

    def kids(self, n):
        if self.flat is not None:
            return [self.flat[i] for i in n.get("childrenRefs", [])]
        return n.get("children", [])

    def source(self, n):
        cls = n.get("className", "")
        s = (self.method_src.get(f"{cls};{n.get('methodName')};{n.get('methodDesc', '')}")
             or self.line_src.get(f"{cls};{n.get('lineNumber')}")
             or self.class_src.get(cls))
        if not s and "$" in cls:
            s = self.class_src.get(cls.split("$", 1)[0])
        return s

    def aggregate(self):
        """自身耗时按方法汇总；按插件汇总时，服务端代码被插件调用的耗时归到最近的插件栈帧。"""
        by_plugin, by_method, method_src = Counter(), Counter(), {}
        plugin_methods, plugin_entries = defaultdict(Counter), defaultdict(Counter)
        stack = [(c, None) for c in self.kids(self.thread)]
        while stack:
            n, inherited = stack.pop()
            own = self.source(n)
            plugin = own or inherited
            children = self.kids(n)
            self_time = tot(n) - sum(tot(c) for c in children)
            by_plugin[plugin or "(服务端/JDK)"] += self_time
            key = f"{n.get('className')}.{n.get('methodName')}"
            by_method[key] += self_time
            if plugin:
                plugin_methods[plugin][key] += self_time
            if own and own != inherited:
                plugin_entries[own][key] += tot(n)
            if own:
                method_src[key] = own
            elif IDLE_METHODS.match(n.get("methodName", "")):
                method_src[key] = "等待/空闲"
            stack.extend((c, plugin) for c in children)
        return by_plugin, by_method, method_src, plugin_methods, plugin_entries

    def hot_paths(self, min_pct, max_lines, max_depth=20):
        out, threshold = [], self.total * min_pct / 100

        def sig(n):
            return sorted((c for c in self.kids(n) if tot(c) >= threshold), key=tot, reverse=True)

        def label(chain):
            names = []
            for n in chain:
                s = self.source(n)
                names.append(short(n) + (f" [{s}]" if s else ""))
            if len(names) > 6:
                names = names[:2] + [f"…({len(names) - 5} 层)…"] + names[-3:]
            return " → ".join(names)

        def visit(n, depth):
            if len(out) >= max_lines:
                return
            chain, children = [n], sig(n)
            while len(children) == 1 and tot(children[0]) >= tot(chain[-1]) * 0.9:
                chain.append(children[0])
                children = sig(children[0])
            head, tail = tot(chain[0]), tot(chain[-1])
            ratio = pct(head, self.total) + (f"→{pct(tail, self.total)}" if head - tail > threshold else "")
            out.append("  " * depth + f"- {ratio} {label(chain)}")
            if depth < max_depth:
                for c in children:
                    visit(c, depth + 1)

        for root in sorted(self.kids(self.thread), key=tot, reverse=True):
            if tot(root) >= threshold:
                visit(root, 0)
        if len(out) >= max_lines:
            out.append(f"- …（已截断，仅显示前 {max_lines} 行）")
        return out


# ---------- 配置提取 ----------

def flatten(obj, prefix=""):
    if isinstance(obj, dict):
        for k, v in obj.items():
            yield from flatten(v, f"{prefix}.{k}" if prefix else str(k))
    elif isinstance(obj, list):
        if len(obj) <= 6 and all(not isinstance(x, (dict, list)) for x in obj):
            yield prefix, obj
    else:
        yield prefix, obj


def extract_configs(raw):
    files = {}
    for name, content in (raw or {}).items():
        try:
            parsed = json.loads(content) if isinstance(content, str) else content
        except json.JSONDecodeError:
            continue
        if name.endswith("/") and isinstance(parsed, dict):
            for sub, sub_content in parsed.items():
                files[name + sub] = sub_content
        else:
            files[name] = parsed
    result = {}
    for name, content in files.items():
        # paper 中 "default" / -1 表示沿用默认或上级配置，没有信息量
        hits = [(k.replace("world-settings.default.", "default."), v) for k, v in flatten(content)
                if CFG_KEYS.search(k) and "message" not in k
                and v != "default" and v != [] and not (name.startswith("paper/") and v == -1)]
        if hits:
            result[name] = hits
    return result


# ---------- 历史时间线 ----------

def avg(xs):
    return sum(xs) / len(xs)


TIMELINE_COLS = [
    ("TPS均值", "tps", lambda v: v, avg),
    ("TPS最低", "tps", lambda v: v, min),
    ("MSPT中位", "tickDuration", lambda v: v["median"], avg),
    ("MSPT p95", "tickDuration", lambda v: v["percentile95"], avg),
    ("MSPT最大", "tickDuration", lambda v: v["max"], max),
    ("进程CPU%", "cpuUsageProcess", lambda v: v * 100, avg),
    ("系统CPU%", "cpuUsageSystem", lambda v: v * 100, avg),
    ("堆GB峰值", "memoryUsageHeap", lambda v: v["used"] / 1024 ** 3, max),
    ("分配MB/s", "memoryAllocation", lambda v: v / 1024 ** 2, avg),
    ("玩家", "worldInfo", lambda v: v["players"], avg),
    ("实体", "worldInfo", lambda v: v["entities"], avg),
    ("方块实体", "worldInfo", lambda v: v["tileEntities"], avg),
    ("区块", "worldInfo", lambda v: v["chunks"], avg),
    ("延迟中位", "playerPing", lambda v: v["median"], avg),
]


def timeline(metrics, max_rows=15):
    points = defaultdict(list)
    t_min = t_max = None
    for _, key, fn, _ in TIMELINE_COLS:
        s = metrics.get(key) or {}
        t = s.get("startTimestampMs")
        if t is None or key in points:
            continue
        for dt, v in zip(s.get("timestampDeltasMs", []), s.get("values", [])):
            t += dt
            if v is not None:
                points[key].append((t, v))
                t_min = t if t_min is None else min(t_min, t)
                t_max = t if t_max is None else max(t_max, t)
    if not points:
        return None
    span_min = (t_max - t_min) / 60000
    bucket = max(5, -(-int(span_min) // max_rows)) * 60000
    rows = defaultdict(dict)
    for name, key, fn, agg in TIMELINE_COLS:
        groups = defaultdict(list)
        for t, v in points.get(key, []):
            try:
                groups[t // bucket].append(fn(v))
            except (KeyError, TypeError):
                pass
        for b, vals in groups.items():
            rows[b][name] = agg(vals)
    cols = [c[0] for c in TIMELINE_COLS if any(c[0] in r for r in rows.values())]
    out = [f"| 时间(本机时区) | {' | '.join(cols)} |", "|" + "---|" * (len(cols) + 1)]
    for b in sorted(rows):
        cells = []
        for c in cols:
            v = rows[b].get(c)
            cells.append("-" if v is None else (f"{v:.0f}" if abs(v) >= 100 else f"{v:.1f}"))
        out.append(f"| {datetime.fromtimestamp(b * bucket / 1000):%H:%M} | {' | '.join(cells)} |")
    return out, bucket // 60000, span_min


# ---------- 摘要生成 ----------

def build_summary(d, code, min_pct, max_lines):
    L, findings = [], []
    add = L.append
    m = d.get("metadata", {})
    plat = m.get("platform", {})
    ps = m.get("platformStatistics", {})
    ss = m.get("systemStatistics", {})
    tws = d.get("timeWindowStatistics") or {}
    duration = (m.get("endTime", 0) - m.get("startTime", 0)) / 1000

    # 概览
    add(f"# spark 报告摘要 `{code}`")
    add("\n## 概览")
    add(f"- 类型: {d.get('type')} | 平台: {plat.get('brand') or plat.get('name')} ({plat.get('name')} {plat.get('version')}) | MC {plat.get('minecraftVersion')}")
    if d.get("type") == "sampler":
        mode = {0: "CPU执行", 1: "内存分配"}.get(m.get("samplerMode"), m.get("samplerMode"))
        engine = {0: "java", 1: "async-profiler"}.get(m.get("samplerEngine"), m.get("samplerEngine"))
        add(f"- 采样: {duration:.0f}秒, 间隔 {m.get('interval', 0) / 1000:g}ms, 模式 {mode}, 引擎 {engine}, ticks {m.get('numberOfTicks')}")
        thr = (m.get("dataAggregator") or {}).get("tickLengthThreshold")
        if thr:
            add(f"- 注意: 仅统计耗时超过 {thr}ms 的 tick（--only-ticks-over）")
    if m.get("comment"):
        add(f"- 备注: {m['comment']}")
    if "uptime" in ps:
        add(f"- 运行时长: {ps['uptime'] / 3600000:.1f}小时 | 在线玩家: {ps.get('playerCount')} | 正版验证: {ps.get('onlineMode')}")
    os_ = ss.get("os", {})
    cpu = ss.get("cpu", {})
    java = ss.get("java", {})
    if ss:
        add(f"- 系统: {os_.get('name')} {os_.get('arch')} | CPU: {cpu.get('modelName', '').strip()} ({cpu.get('threads')} 线程)")
        args = java.get("vmArgs", "")
        flags = re.findall(r"-Xm[sx]\S+|-XX:\+Use\w+GC|-XX:MaxGCPauseMillis=\d+", args)
        if "aikars" in args:
            flags.append("Aikar flags")
        add(f"- Java: {java.get('vendor')} {java.get('version')} | 关键参数: {' '.join(flags) or '(无)'}")
        if args:
            add(f"- 完整 JVM 参数: `{args}`")

    # 性能
    add("\n## 性能指标")
    tps = ps.get("tps") or {}
    if tps:
        add(f"- TPS: 1m {f1(tps.get('last1m', 0))} / 5m {f1(tps.get('last5m', 0))} / 15m {f1(tps.get('last15m', 0))}")
    mspt = ps.get("mspt") or {}
    for k in ("last1m", "last5m"):
        if k in mspt:
            add(f"- MSPT {k}: {stat_line(mspt[k])} ms")
    for wid, w in tws.items():
        add(f"- 时间窗口 {wid}: {w.get('duration', 0) / 1000:.0f}秒, TPS {f1(w.get('tps', 0))}, MSPT 中位 {f1(w.get('msptMedian', 0))} / 最大 {f1(w.get('msptMax', 0))}, "
            f"CPU {pct(w.get('cpuProcess', 0), 1)}, 玩家 {w.get('players')}, 实体 {w.get('entities')}, 方块实体 {w.get('tileEntities')}, 区块 {w.get('chunks')}")
    if cpu:
        pu, su = cpu.get("processUsage", {}), cpu.get("systemUsage", {})
        add(f"- CPU: 进程 1m {pct(pu.get('last1m', 0), 1)} / 15m {pct(pu.get('last15m', 0), 1)} | 系统 1m {pct(su.get('last1m', 0), 1)} / 15m {pct(su.get('last15m', 0), 1)}")
    heap = (ps.get("memory") or {}).get("heap")
    nonheap = (ps.get("memory") or {}).get("nonHeap") or {}
    if heap:
        add(f"- 堆内存: 已用 {gb(heap['used'])} / 已分配 {gb(heap['committed'])} / 最大 {gb(heap['max'])} | 非堆 {gb(nonheap.get('used', 0))}")
    for pool in (ps.get("memory") or {}).get("pools") or []:
        u, cu = pool.get("usage") or {}, pool.get("collectionUsage") or {}
        mx = gb(u["max"]) if u.get("max", -1) > 0 else "无上限"
        line = f"  - 内存池 {pool.get('name')}: 已用 {gb(u.get('used', 0))} / 已分配 {gb(u.get('committed', 0))} / 最大 {mx}"
        if cu.get("used"):
            line += f" | 上次GC后 {gb(cu['used'])}"
        add(line)
    smem = ss.get("memory", {})
    phys, swap = smem.get("physical"), smem.get("swap")
    if phys:
        add(f"- 物理内存: {gb(phys['used'])} / {gb(phys['total'])} | Swap: {gb(swap['used'])} / {gb(swap['total'])}" if swap else
            f"- 物理内存: {gb(phys['used'])} / {gb(phys['total'])}")
    gc_lines = [f"{n}: {g['total']}次, 平均 {g['avgTime']:.0f}ms, 间隔 {g['avgFrequency'] / 1000:.1f}s"
                for n, g in (ss.get("gc") or {}).items() if g.get("total")]
    if gc_lines:
        add("- GC(启动以来): " + " | ".join(gc_lines))
    gc_sample = [f"{n}: {g['total']}次, 平均 {g['avgTime']:.0f}ms" for n, g in (ps.get("gc") or {}).items() if g.get("total")]
    if gc_sample:
        add("- GC(采样期间): " + " | ".join(gc_sample))
    ping = (ps.get("ping") or {}).get("last15m")
    if ping:
        add(f"- 玩家延迟(15m): {stat_line(ping)} ms")
    disk = ss.get("disk")
    if disk and disk.get("total"):
        add(f"- 磁盘: {gb(disk['used'])} / {gb(disk['total'])}")

    tl = timeline(m.get("metrics") or {})
    if tl:
        rows, bucket_min, span_min = tl
        add(f"\n## 历史时间线（采样前约 {span_min:.0f} 分钟，每 {bucket_min} 分钟一行）")
        L.extend(rows)

    # 诊断规则
    if tps.get("last1m") is not None:
        t1 = tps["last1m"]
        if t1 < 18:
            findings.append(("严重", f"TPS 1分钟均值仅 {f1(t1)}，服务器明显卡顿"))
        elif t1 < 19.5:
            findings.append(("警告", f"TPS 1分钟均值 {f1(t1)}，略低于 20"))
    if "last1m" in mspt:
        ms = mspt["last1m"]
        if ms["median"] > 50:
            findings.append(("严重", f"MSPT 中位数 {f1(ms['median'])}ms > 50ms，主线程持续超载"))
        elif ms["percentile95"] > 50:
            findings.append(("警告", f"MSPT p95 {f1(ms['percentile95'])}ms > 50ms，存在周期性卡顿"))
        if ms["max"] > 150:
            findings.append(("提示", f"出现 {f1(ms['max'])}ms 的单 tick 尖峰"))
    if swap and swap.get("used", 0) > 1024 ** 3:
        findings.append(("严重", f"系统 Swap 已使用 {gb(swap['used'])}，JVM 内存被换出到磁盘会导致严重卡顿和 GC 变慢"))
    if phys and phys["used"] / phys["total"] > 0.9:
        findings.append(("警告", f"物理内存使用 {pct(phys['used'], phys['total'])}"))
    if heap and phys and heap["max"] + nonheap.get("used", 0) > phys["total"] * 0.85:
        findings.append(("严重", f"最大堆 {gb(heap['max'])} + 非堆 {gb(nonheap.get('used', 0))} 接近/超过物理内存 {gb(phys['total'])}，内存超配"))
    if heap and heap["max"] > 0 and heap["used"] / heap["max"] > 0.9:
        findings.append(("警告", f"堆内存使用率 {pct(heap['used'], heap['max'])}"))
    for n, g in (ss.get("gc") or {}).items():
        if ("Old" in n or "Full" in n or "MarkSweep" in n) and g.get("total"):
            findings.append(("警告", f"{n} 发生 {g['total']} 次（平均 {g['avgTime']:.0f}ms），存在长停顿"))
        elif "Young" in n and g.get("avgTime", 0) > 50:
            findings.append(("警告", f"{n} 平均停顿 {g['avgTime']:.0f}ms 偏高（理想 <50ms）"))
    if cpu.get("threads") and cpu["threads"] <= 4:
        findings.append(("警告", f"仅 {cpu['threads']} 个 CPU 线程，异步任务/GC 会与主线程争抢"))
    if (cpu.get("processUsage") or {}).get("last1m", 0) > 0.9:
        findings.append(("警告", "进程 CPU 占用超过 90%"))
    jv = re.match(r"(\d+)", java.get("version", ""))
    if jv and int(jv.group(1)) < 21:
        findings.append(("警告", f"Java 版本 {java['version']} 较旧，建议 21+"))
    if d.get("type") == "sampler" and 0 < duration < 120:
        findings.append(("提示", f"采样仅 {duration:.0f} 秒，结果可能不具代表性（建议 ≥5 分钟）"))

    # CPU 时间分布
    threads = sorted(d.get("threads") or [], key=tot, reverse=True)
    for th in threads[:3]:
        ta = ThreadAnalyzer(th, d)
        if not ta.total:
            continue
        by_plugin, by_method, msrc, plugin_methods, plugin_entries = ta.aggregate()
        add(f"\n## 线程「{th.get('name')}」CPU 时间分布")
        add("### 按插件/模组（含其调用的服务端代码）")
        for name, t in by_plugin.most_common(30):
            if t / ta.total >= 0.0005:
                add(f"- {pct(t, ta.total)} {name}")
                if name != "(服务端/JDK)" and t / ta.total >= 0.03:
                    findings.append(("警告", f"插件 {name} 占用线程「{th.get('name')}」{pct(t, ta.total)} 的时间"))
                elif name != "(服务端/JDK)" and t / ta.total >= 0.01:
                    findings.append(("提示", f"插件 {name} 占用 {pct(t, ta.total)}"))
        details = [p for p, t in by_plugin.most_common() if p in plugin_entries and t / ta.total >= 0.001]
        if details:
            add("### 插件耗时明细（入口=插件被调用处的总耗时，热点=归属该插件的自身耗时）")
            for p in details:
                add(f"- **{p}** ({pct(by_plugin[p], ta.total)})")
                for label, counter in (("入口", plugin_entries[p]), ("热点", plugin_methods[p])):
                    items = [f"{short_key(k)} {pct(t, ta.total)}" for k, t in counter.most_common(5) if t / ta.total >= 0.0005]
                    if items:
                        add(f"  - {label}: " + "; ".join(items))
        add("### 自身耗时最高的方法")
        for name, t in by_method.most_common(40):
            tag = f" [{msrc[name]}]" if name in msrc else ""
            add(f"- {pct(t, ta.total)} {name}{tag}")
        add(f"### 热点调用路径（≥{min_pct}%，单子链已折叠，A→B 表示链首/链尾占比）")
        L.extend(ta.hot_paths(min_pct, max_lines))

    # 实体
    world = ps.get("world") or {}
    if world:
        add("\n## 实体与区块")
        counts = Counter(world.get("entityCounts") or {})
        add(f"- 实体总数 {world.get('totalEntities')}: " + ", ".join(f"{k} {v}" for k, v in counts.most_common(30)))
        if counts.get("item", 0) > 1000:
            findings.append(("警告", f"掉落物 {counts['item']} 个，偏多"))
        if counts.get("experience_orb", 0) > 500:
            findings.append(("警告", f"经验球 {counts['experience_orb']} 个，偏多"))
        chunks, areas, area_types = [], Counter(), defaultdict(Counter)
        for w in world.get("worlds") or []:
            wname = w.get("name")
            per_world = Counter()
            for region in w.get("regions") or []:
                for c in region.get("chunks") or []:
                    chunks.append((wname, c))
                    per_world.update(c.get("entityCounts") or {})
                    area = (wname, c["x"] // 8, c["z"] // 8)
                    areas[area] += c.get("totalEntities", 0)
                    area_types[area].update(c.get("entityCounts") or {})
            add(f"- 世界 {wname}: {w.get('totalEntities')} 实体" +
                (": " + ", ".join(f"{k} {v}" for k, v in per_world.most_common(10)) if per_world else ""))
        chunks.sort(key=lambda x: x[1].get("totalEntities", 0), reverse=True)
        if chunks:
            add("- 实体最密集的区块（方块坐标）:")
            for wname, c in chunks[:25]:
                top = Counter(c.get("entityCounts") or {}).most_common(3)
                add(f"  - {wname} ({c['x'] * 16}, {c['z'] * 16}): {c['totalEntities']} 个，" + ", ".join(f"{k} {v}" for k, v in top))
            dense = [(w, c) for w, c in chunks if c.get("totalEntities", 0) >= 100]
            if dense:
                findings.append(("警告", f"{len(dense)} 个区块实体数 ≥100，最多 {dense[0][1]['totalEntities']} 个，位于 {dense[0][0]} ({dense[0][1]['x'] * 16}, {dense[0][1]['z'] * 16})"))
            add("- 实体最密集的 128×128 区域（可定位玩家基地/农场）:")
            for (wname, ax, az), n in areas.most_common(15):
                top = area_types[(wname, ax, az)].most_common(4)
                add(f"  - {wname} x {ax * 128}~{ax * 128 + 127}, z {az * 128}~{az * 128 + 127}: {n} 个，" + ", ".join(f"{k} {v}" for k, v in top))
        changed = []
        for rule in world.get("gameRules") or []:
            diff = {w: v for w, v in (rule.get("worldValues") or {}).items() if v != rule.get("defaultValue")}
            if diff:
                changed.append(f"{rule['name']}(默认{rule.get('defaultValue')}): " + ",".join(f"{w}={v}" for w, v in diff.items()))
        if changed:
            add("- 非默认游戏规则: " + "; ".join(changed[:40]))
        packs = [p["name"] for p in world.get("dataPacks") or [] if not p.get("builtIn") and p.get("source") != "builtin"]
        if packs:
            add("- 数据包: " + ", ".join(packs))

    # 配置
    cfgs = extract_configs(m.get("serverConfigurations"))
    if cfgs:
        add("\n## 关键性能配置")
        props = dict(cfgs.get("server.properties", []))
        vd, sd = props.get("view-distance"), props.get("simulation-distance")
        if isinstance(vd, int) and vd > 10:
            findings.append(("提示", f"view-distance={vd} 偏大"))
        if isinstance(sd, int) and sd > 8:
            findings.append(("提示", f"simulation-distance={sd} 偏大"))
        for name, hits in cfgs.items():
            shown = "; ".join(f"{k}={json.dumps(v, ensure_ascii=False)}" for k, v in hits[:CFG_MAX_PER_FILE])
            more = f" …(+{len(hits) - CFG_MAX_PER_FILE})" if len(hits) > CFG_MAX_PER_FILE else ""
            add(f"- **{name}**: {shown}{more}")

    # 插件
    sources = [s for s in (m.get("sources") or {}).values() if not s.get("builtIn")]
    if sources:
        add(f"\n## 插件/模组列表（{len(sources)} 个）")
        for s in sorted(sources, key=lambda s: str(s.get("name")).lower()):
            desc = re.sub(r"\s+", " ", s.get("description") or "").strip()
            add(f"- {s.get('name')} {s.get('version')}" + (f": {desc[:100]}" if desc else ""))

    if d.get("type") != "sampler":
        add(f"\n> 注意: 报告类型为 `{d.get('type')}`，当前脚本只完整支持 sampler 报告，调用树部分已跳过。")

    # 诊断置顶
    order = {"严重": 0, "警告": 1, "提示": 2}
    findings.sort(key=lambda x: order[x[0]])
    diag = ["\n## 自动诊断"] + [f"- [{lv}] {msg}" for lv, msg in findings] if findings else []
    return "\n".join(L[:1] + diag + L[1:]) + "\n"


def estimate_tokens(text):
    try:
        import tiktoken
        return len(tiktoken.get_encoding("cl100k_base").encode(text)), "tiktoken"
    except ImportError:
        ascii_chars = sum(1 for ch in text if ord(ch) < 128)
        return int(ascii_chars / 4 + (len(text) - ascii_chars)), "粗略估算"


def main():
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8")
        sys.stderr.reconfigure(encoding="utf-8")
    ap = argparse.ArgumentParser(description="将 spark 报告压缩为 LLM 可读的摘要")
    ap.add_argument("source", nargs="?", help="spark 链接 / 报告ID / 本地 JSON 文件")
    ap.add_argument("-o", "--output", help="输出 Markdown 路径（默认 summary_<ID>.md）")
    ap.add_argument("--proxy", help="HTTP 代理，例如 http://127.0.0.1:7897")
    ap.add_argument("--no-cache", action="store_true", help="忽略本地缓存重新下载")
    ap.add_argument("--min-pct", type=float, default=0.5, help="热点路径最小占比（默认 0.5）")
    ap.add_argument("--max-lines", type=int, default=150, help="热点路径最多行数（默认 150）")
    args = ap.parse_args()

    src = args.source or input("请输入 spark 链接: ").strip()

    data, code = load_report(src, args.proxy, not args.no_cache)
    summary = build_summary(data, code, args.min_pct, args.max_lines)
    out = args.output or f"summary_{code}.md"
    with open(out, "w", encoding="utf-8") as f:
        f.write(summary)

    print(summary)
    raw_len = len(json.dumps(data, ensure_ascii=False))
    tokens, method = estimate_tokens(summary)
    print(f"[原始 JSON {raw_len / 1024:.0f}KB → 摘要 {len(summary.encode('utf-8')) / 1024:.1f}KB，约 {tokens} tokens（{method}），已保存到 {out}]", file=sys.stderr)


if __name__ == "__main__":
    main()
