# ADR-0004：数据正确性契约：链接过滤、配置合并、解析缓存与信号顺序

| 项目 | 内容 |
|------|------|
| 状态 | **Accepted（2026-11-05）** |
| 日期 | 2026-11-05（起草并接受） |
| 基线 | `master @ 5f12736`（ADR-0001/0002/0003 均已 Accepted 并落地；测试基线 `124 passed`） |
| 关联问题 | **P1-05（主）**、P1-06、P1-07、P1-08、P1-15、P1-16、P1-17、P2-15、P2-16、P2-17；部分顺带 P2-03（仅 scheme deny 一项） |
| 关联文档 | [08 §8.11 阶段 3](../wiki/08-issues-and-recommendations.md)、[03 主链路](../wiki/03-crawler.md)、[01 架构](../wiki/01-architecture.md)、[06 依赖](../wiki/06-dependencies.md)、[ADR-0003](0003-shutdown-cancellation-and-loop-quiescence.md) |
| 决策者 | 项目维护者 |

> 本文冻结「可选层数据路径上，中间值是什么类型、空配置等于什么、缓存的身份与统计口径、同优先级事件按什么顺序执行」的契约，**不包含代码变更**。文中的类型与签名是方案描述，实施另行提交。

## 1. 背景

8.11 的阶段 3 被归为「数据正确性」：六组问题都不涉及并发与生命周期，而是**同一个值在不同层被当成不同东西**。

- 链接过滤里 `link` 有时是 `Link`、有时是 `str`，`urlparse` 在 `normalize=False` 时直接崩溃；
- 「空配置」在 host 与 extension 两处被解释成「全部拒绝」，而调用方的直觉是「不过滤」；
- `Config` 的递归合并始终写回顶层，默认值又是浅拷贝；
- `Parser` 的缓存 key 含 `self`，统计把每次调用都算命中；
- 异步信号用 `(priority, handler)` 元组直接排序，同优先级时比较函数对象；`sender=None` 时同一列表被拼两次。

阶段 2 已经把运行期与终态的资源所有权冻结（ADR-0002/0003）。阶段 3 之后，阶段 4 要把 `Config`、`MiddlewareManager`、`PersisterManager` 与异步 `SignalManager` 接进 `Spider`（P1-11）。如果这四层的中间值语义仍是隐式的，阶段 4 会把错误语义固化到主链路里，返工成本远高于现在。

### 1.1 现状

| 环节 | 实现位置 | 当前行为 | 缺口 |
|------|----------|----------|------|
| 模块级链接过滤 | `extractor.py:224-357` | `normalize=True` 时把 `link` 就地变成 `str`；`False` 时仍是 `Link` | 同一变量两种类型，`urlparse(Link)` 崩溃（P1-05） |
| 模块级 host 检查 | `extractor.py:275-280,326-329` | `_host_allowed_lenient` 在 `hosts_allow` 为空时返回 `False` | 空配置 → 全量拒绝（P1-15） |
| 模块级扩展名检查 | `extractor.py:253-259,335-336` | 空扩展名恒 `False` | 无路径 URL 被误杀（P1-16） |
| 类级 host 检查 | `extractor.py:845-850,909-918` | `lenient=True` 且无 `hosts_allow` 时全量拒绝 | 与 P1-15 同源（P1-17） |
| 类级输出 | `extractor.py:889-897,932-949` | 归一化只作用于局部变量，返回的 `Link` 仍是原始 URL | `normalize=True` 对输出无效（P1-05） |
| 模块级 scheme deny | `extractor.py:301-305` | 用 `schemes_allow` 判断是否执行 deny | deny 语义在模块级失效（P2-03 的一项） |
| Config 初始化 | `config.py:92` | `DEFAULT_CONFIG.copy()` 浅拷贝 | 实例间共享嵌套默认值（P1-06） |
| Config 合并 | `config.py:157-171` | 递归调用始终写 `self._config` | 嵌套键被拍平到顶层（P1-06） |
| 环境变量 | `config.py:122-155` | 按 `.` 拆分键名；环境变量名不能含 `.` | 嵌套覆盖永远不生效（P2-16） |
| 环境变量类型 | `config.py:145-155,173-198` | 非数字字符串原样写入；校验直接与 `int` 比较 | `TypeError: '<' not supported ...`（E15） |
| 解析缓存 | `parser.py:74-76,156-160` | `lru_cache` 修饰实例方法，key 含 `self` 与全文 | 实例被类级缓存持有；首次解析即计命中（P1-07） |
| 缓存返回值 | `parser.py:74-96,156-160` | 同一实例同内容返回同一棵可变树 | 调用方修改污染后续解析（P2-17） |
| 信号排序 | `signals.py:68,87-105` | `sorted((priority, handler))`，升序 | 同优先级比较函数对象抛 `TypeError`；方向与「higher = earlier」相反（P1-08） |
| 信号接收者合并 | `signals.py:130-146` | `sender=None` 时拼接 `_receivers[None]` 两次 | 同一处理器执行两遍（P2-15） |

### 1.2 实测证据（2026-11-05，`master @ 5f12736`）

> 以下为本地只读探针实测，非静态推断。探针通过 `PYTHONPATH=. uv run python <temp>.py` 运行，不联网、不修改仓库，运行后已移入回收站。

| # | 场景 | 实测结果 |
|---|------|----------|
| E1 | `extract_and_filter_links('<a href="/p">x</a>', base_url="http://example.com")` | `[]` —— 默认参数即返回空（P1-15） |
| E2 | `FilteringLinkExtractor(normalize=False).extract(HTML)` | `AttributeError: 'Link' object has no attribute 'decode'`（P1-05） |
| E3 | `normalize=True` 提取 `HTTP://Example.COM:80/p?b=2&a=1#frag` | 返回 `Link` 的 `url` 仍为原串：`'http://Example.COM:80/p?b=2&a=1#frag'`，未去端口、未排序 query、未去 fragment（P1-05 输出无效） |
| E4 | 模块级过滤，`hosts_allow=["example.com"]`：`http://example.com` vs `http://example.com/p` | 根路径 `[]`，有路径 `[<Link http://example.com/p>]` —— 空扩展名被拒（P1-16） |
| E5 | `FilteringLinkExtractor(lenient=True)` 不配 `hosts_allow`；`lenient=False` 同输入 | lenient `[]`，strict 返回 3 条 —— lenient 空配置全拒（P1-17） |
| E6 | `c._merge_config({"downloader": {"concurrent_requests": 42}})` | 顶层 `concurrent_requests == 42`，`downloader.concurrent_requests` 仍为 `10`（P1-06） |
| E7 | 构造 `Config` 后把 `c1._config["downloader"]["concurrent_requests"]` 改为 99 | `DEFAULT_CONFIG` 同路径变为 99；新建 `Config` 读到 99（浅拷贝污染，P1-06） |
| E8 | 同时设置 `WEBANTS_DOWNLOADER_CONCURRENT_REQUESTS=42` 与 `WEBANTS_DOWNLOADER__CONCURRENT_REQUESTS=43` | `downloader.concurrent_requests == 10`；顶层多出 `downloader_concurrent_requests=42`、`downloader__concurrent_requests=43` 两个垃圾键（P2-16） |
| E9 | `Parser()` 首次 `parse(content)` | `stats["cache_hits"] == 1`，而 `cache_info()` 为 `hits=0, misses=1`（P1-07） |
| E10 | 同一 `Parser` 实例对相同内容 `parse` 两次，修改第一棵树 | 两次返回同一对象（`a is b == True`），第二次读到 `MUTATED`（P2-17） |
| E11 | `Parser` 实例 `del` 后 `gc.collect()`，持 `weakref` | 实例仍存活；`Parser._cached_parse.cache_clear()` 后才被回收（P1-07 实例泄漏） |
| E12 | 两个接收器均 `priority=0` 时 `send` | `TypeError: '<' not supported between instances of 'function' and 'function'`（P1-08） |
| E13 | 接收器 `priority=0`（low）与 `priority=10`（high） | 执行顺序为 `low, high`，与 `connect` 文档「higher = earlier」相反（P1-08） |
| E14 | 接收器 `connect` 时 `sender` 取默认 `None`，然后 `send(None)` | 处理器执行 **2** 次（P2-15） |
| E15 | `_set_nested_value("downloader.concurrent_requests", "abc")` 后 `_validate_config()` | `TypeError: '<' not supported between instances of 'str' and 'int'`（P2-16 的类型边界，修复 `__` 映射后会被真实环境变量触发） |

### 1.3 决策驱动因素

1. **中间值只能有一种类型**：过滤谓词的输入必须是 `str`；`Link` 只在提取入口与输出重建时存在。当前崩溃不是「某个分支写错」，而是类型契约缺失。
2. **空配置必须等价于「不过滤」**：默认参数可用、lenient 可用，是库的基本预期（E1/E5）。
3. **配置来源可预测**：默认值不可被实例污染；嵌套键合并到嵌套层；环境变量能覆盖嵌套键且失败时可见（E6–E8、E15）。
4. **缓存身份与统计可断言**：命中就是命中，首次解析就是 miss；实例可被回收；调用方拿到的树不影响缓存（E9–E11）。
5. **事件顺序确定且不比较函数对象**：同优先级按注册顺序，高优先级先执行（E12/E13）。
6. **不扩大可选层接线范围**：本 ADR 只冻结数据语义；把 `Config`/异步信号接进 `Spider` 仍属 P1-11（阶段 4），避免一次改动同时引入语义变更与接线变更。

## 2. 决策

### 2.1 链接过滤：单一中间表示与显式判定矩阵

#### 所有权矩阵

| 阶段 | 持有者 | 移交点 | 释放责任 |
|------|--------|--------|----------|
| HTML → `Link` | `LinkExtractor` | `extract()` 返回 `list[Link]` | 无资源 |
| `Link` → 判定字符串 | `FilteringLinkExtractor` / 模块级函数 | `_canonical(link.url)` | 无资源 |
| 谓词判定 | **唯一的** `_LinkFilter.allowed(url: str)` | 返回 `bool` | 无资源 |
| 过滤后处理 | 调用方提供的 `link_process_func(Link) -> Link` | 在判定通过之后 | 无资源 |
| 输出重建 | `FilteringLinkExtractor` | `normalize=True` 时 `Link(normalize_url(...), unique=...)` | 无资源 |

不变量：

- **I1 类型唯一**：所有过滤谓词只接受 `str`；任何 `Link` 在进入谓词前必须取 `.url`。禁止把 `Link` 传给 `urlparse`、`re.search` 或 `normalize_url`。
- **I2 判定与输出解耦**：过滤判定恒使用归一化形式（`_canonical`），因此**判定结果与 `normalize` 参数无关**；`normalize` 只决定返回 `Link` 的 `url` 是否已归一化。
- **I3 空配置放行**：`hosts_allow` 为空时 host 检查通过；`extensions_allow` 为空时扩展名检查通过（含空扩展名）。
- **I4 lenient 只在有列表时生效**：`lenient=True` 仅改变「如何比较」；列表为空时与 strict 一样放行，不得因空列表返回 `False`。
- **I5 扩展名定义**：扩展名取路径最后一段中最后一个 `.` 之后的部分并转小写；路径为空、最后一段不含 `.`、或最后一段只有 `.` 时为 `""`。当前 `path.rsplit(".")[-1]` 会把 `/a/b` 当成扩展名，本 ADR 一并修正。
- **I6 顺序固定**：scheme → host → extension → regex；每一类先 allow 后 deny；任一不通过即拒绝，不短路后续统计（无统计依赖）。

#### 唯一判定核心

```python
class _LinkFilter:
    """内部核心；不导出。所有输入均为 str。"""

    def __init__(
        self,
        *,
        extensions_allow: list[str] | None = None,
        extensions_deny: list[str] | None = None,
        hosts_allow: list[str] | None = None,
        hosts_deny: list[str] | None = None,
        schemes_allow: list[str] | None = None,
        schemes_deny: list[str] | None = None,
        regexps_allow: list[str] | None = None,
        regexps_deny: list[str] | None = None,
        lenient: bool = False,
    ): ...

    def allowed(self, url: str) -> bool: ...
```

- 模块级 `extract_and_filter_links` 与 `FilteringLinkExtractor` 都只构造它，不再各自维护一套 `_host_allowed*` / `_extension_allowed*`。
- 模块级函数**新增关键字参数** `lenient: bool = False`，与类保持同语义（原先同时执行 strict 与 lenient 两套检查，是 E1 的直接原因之一）。
- 模块级函数暂不新增 regexps 参数：核心支持，但公开签名不变，避免阶段 3 扩大 API 面。

#### 归一化与输出

```python
def _canonical(url: str) -> str:
    try:
        return normalize_url(url)
    except Exception:
        return url          # 归一化失败不改变可判定性；后续 host/scheme 检查照常
```

```python
for link in extracted:
    if not filter_obj.allowed(_canonical(link.url)):
        continue
    link = link_process_func(link)
    if normalize:
        link = Link(normalize_url(link.url), unique=link.unique)
    output.append(link)
```

要点：

- 判定使用 canonical、输出按 `normalize` 重建，修复「局部变量变字符串、返回对象没变」（E3）。
- `_canonical` 失败时回退原串：归一化是输出加工，不应让一条坏 URL 触发整个提取失败；坏 URL 随后仍会在 `host`/`scheme` 检查处被拒。
- **P2-03 部分顺带**：模块级 `_scheme_denied` 改为 `if schemes_deny: return scheme in schemes_deny`。P2-03 的另外两项（`except AttribExtractor`、恒真的 `if link := Link(...)`）不在本 ADR 范围，保持 Open。

#### lenient 判定表（`hosts_allow` × `lenient`）

| `hosts_allow` | `lenient` | `example.com` 对 `www.example.com` | 空配置时任意 host |
|---------------|-----------|-----------------------------------|-------------------|
| 空 | False | 通过 | 通过 |
| 空 | True | 通过 | 通过 |
| 非空 | False | 拒绝（精确匹配） | 按列表判定 |
| 非空 | True | 通过（`lenient_host` 比较） | 按列表判定 |

`hosts_deny` 与 `schemes` 采用同一结构：列表为空 → 不拒绝；`lenient` 只改变 `hosts_deny` 的比较方式。

### 2.2 Config：深拷贝默认值、递归深合并、`__` 环境变量分隔符

```python
import copy

self._config = copy.deepcopy(DEFAULT_CONFIG)

def _deep_merge(self, base: dict, patch: dict) -> None:
    for key, value in patch.items():
        if isinstance(value, dict) and isinstance(base.get(key), dict):
            self._deep_merge(base[key], value)
        else:
            base[key] = copy.deepcopy(value)

def _merge_config(self, config: dict) -> None:
    self._deep_merge(self._config, config)
```

- `_deep_merge` 的目标是**当前层级的 dict**，不再恒写 `self._config`；`_merge_config` 只是入口别名，保持现有调用方不变。
- 默认值深拷贝后，实例的嵌套修改不再回写 `DEFAULT_CONFIG`（E7）。
- `as_dict()` 返回 `copy.deepcopy(self._config)`，调用方修改返回值不再穿透到内部状态（顺带修正，原实现只做浅拷贝）。

环境变量：

- 前缀仍为 `WEBANTS_`；去掉前缀后转小写，**`__` 为层级分隔符**：
  `WEBANTS_DOWNLOADER__CONCURRENT_REQUESTS` → `config["downloader"]["concurrent_requests"]`。
- 单下划线保留为键名的一部分（键本身含 `concurrent_requests` 等），不参与分层。
- 空段（如 `WEBANTS__FOO`、`WEBANTS_A____B`）整条忽略并 `warning`。
- **未知顶层键忽略并 `warning`**：第一段不在 `self._config` 中时不写入。这样 `WEBANTS_DOWNLOADER_CONCURRENT_REQUESTS` 这类旧写法不再产生顶层垃圾键（E8），而是给出可操作的告警。
- 优先级保持「默认值 < YAML < 环境变量」。
- 标量转换规则保持不变：`true/false`（大小写不敏感）→ `bool`；纯数字 → `int`；小数 → `float`；其余为 `str`。
- **类型守卫**：若转换结果与目标路径上默认值的类型不一致，跳过该环境变量并 `warning`，保留前一层来源的值。`_validate_config` 中所有数值比较前先做 `isinstance(..., (int, float))` 判断，非法值回退默认值并告警。这是 E15 的修复点，避免「设置错一个环境变量导致进程起不来」。
- `get()` / `set()` 的公开 API 仍使用 `.` 分隔（如 `set("downloader.concurrent_requests", 42)`）；与环境的 `__` 差异写入配置文档，避免歧义。

### 2.3 Parser：实例级 LRU、纯哈希 key、命中差统计、返回副本

```python
class Parser:
    def __init__(self, ..., cache_size: int | None = None):
        self.cache_size = cache_size if cache_size is not None else lru_cache_limit
        self._cache: OrderedDict[str, etree._ElementBase] = OrderedDict()
        self._cache_hits = 0
        self._cache_misses = 0
```

`parse()` 顺序：

1. `total_parses += 1`；
2. `bytes` 解码（逻辑不变，含 charset 回退）；
3. 大小检查（缓存查询**之前**，保证 `max_document_size` 对所有输入生效）；
4. `key = sha1(content).hexdigest()`；
5. 命中：`move_to_end(key)`、`cache_hits += 1`、返回 `copy.deepcopy(cached)`；
6. 未命中：`cache_misses += 1`，解析新树，写入缓存（存的是这棵新树），按 `cache_size` 从头部淘汰，返回 `copy.deepcopy(tree)`。

不变量：

- **I7 缓存不逃逸**：缓存中的树对象永不直接交给调用方；每次 `parse()` 返回深拷贝。因此调用方修改返回树不影响后续解析（E10 修复），也不存在跨实例共享（E11）。
- **I8 缓存归属实例**：key 只含内容哈希，不含 `self`；缓存由 `Parser` 实例持有，实例被回收时缓存随之回收。
- **I9 统计口径**：`cache_hits` 只在真正命中时递增；首次解析计入 `cache_misses`，不再把 miss 算成 hit（E9）。
- **I10 选择器统计**：`css_cache_hits` / `xpath_cache_hits` 只在「编译结果已存在、直接复用」时递增；首次编译计入新增的 `css_cache_misses` / `xpath_cache_misses`。`total_css_queries` / `total_xpath_queries` 仍为调用次数。
- `cache_size` 构造参数默认 `lru_cache_limit`（1000）；本 ADR **不**把它接到 `Config`（`Config` 尚未接入 Spider，P1-11 阶段 4）。
- 移除私有方法 `_cached_parse`。`get_stats()["cache_info"]` 保留原有键形状（`hits/misses/maxsize/currsize`），由实例计数器与字典长度生成，外部只依赖 `get_stats()`。

### 2.4 异步信号：自增序号 + 显式排序键 + sender 合并修正

接收器存储改为 `(priority: int, seq: int, handler: Handler)`：

```python
self._receivers: dict[Sender | None, list[tuple[int, int, Handler]]] = defaultdict(list)
self._seq = itertools.count()

def connect(self, receiver, sender=None, priority=0):
    ...
    self._receivers[sender].append((priority, next(self._seq), receiver))

def _get_receivers(self, sender):
    if sender is None:
        return list(self._receivers.get(None, ()))
    return list(self._receivers.get(sender, ())) + list(self._receivers.get(None, ()))

for priority, seq, receiver in sorted(receivers, key=lambda r: (-r[0], r[1])):
    ...
```

不变量：

- **I11 可比较且稳定**：排序键只包含 `int`，永不超过到函数对象；同优先级按 `connect` 调用顺序执行（E12）。
- **I12 方向固定**：`priority` 数值越大越先执行，与 `connect` 文档一致（E13）。
- **I13 sender=None 只取一次**：`send(None)` 只遍历 `_receivers[None]`；`send(sender)` 遍历「该 sender 的接收器 + 全局接收器」，各一次（E14）。
- **I14 显式重复注册仍执行多次**：同一 handler 分别注册到具体 sender 和 `None`，在 `send(specific_sender)` 时执行两次；这是两次注册的语义，不做隐式去重。只修 `None + None` 的拼接错误。
- `disable()` / `disconnect()` 的对外语义不变；`send` 的返回值仍为按执行顺序排列的 `(handler, result)` 列表。
- `_get_receivers` 改用 `.get()`，避免 `send` 触碰 `defaultdict` 生成空条目（实现细节，无外部行为）。

### 2.5 统计与观测

| 指标 | 归属 | 变更 |
|------|------|------|
| `cache_hits` | Parser | **口径修正**：仅真实命中 |
| `cache_misses` | Parser | **新增**：真实解析次数 |
| `cache_info` | Parser `get_stats()` | 保留键形状，来源改为实例缓存 |
| `css_cache_hits` / `xpath_cache_hits` | Parser | **口径修正**：仅复用已有编译结果 |
| `css_cache_misses` / `xpath_cache_misses` | Parser | **新增** |
| 链接过滤 | — | 不新增计数；判定完全由入参与配置决定 |
| Config | — | 不新增计数；非法环境变量走 `warning` |
| Signal | — | 不新增计数；顺序由 `connect` 序号决定 |

## 3. 目标状态

### 3.1 链接过滤流水线

```mermaid
flowchart LR
    H[HTML] --> E[LinkExtractor]
    E -->|list[Link]| C[_canonical link.url]
    C --> F{_LinkFilter.allowed str}
    F -- 拒绝 --> X[丢弃]
    F -- 通过 --> P[link_process_func Link]
    P --> N{normalize?}
    N -- 是 --> R1[Link normalize_url url]
    N -- 否 --> R2[Link 原 url]
    R1 --> O[list[Link]]
    R2 --> O
```

### 3.2 可执行对账断言

```python
# 链接过滤
assert extract_and_filter_links('<a href="/p">x</a>', base_url="http://example.com")
assert FilteringLinkExtractor(base_url="http://example.com", normalize=False).extract(html)
assert all(":80" not in l.url and "#" not in l.url for l in FilteringLinkExtractor(normalize=True).extract(html))

# 配置
c = Config()
c._merge_config({"downloader": {"concurrent_requests": 42}})
assert "concurrent_requests" not in c.as_dict()
assert c.get("downloader.concurrent_requests") == 42
assert DEFAULT_CONFIG["downloader"]["concurrent_requests"] == 10

# 解析缓存
p = Parser()
p.parse("<html><body>x</body></html>")
assert p.get_stats()["cache_hits"] == 0
assert p.get_stats()["cache_misses"] == 1
t1 = p.parse("<html><body>y</body></html>")
t1.find(".//body").text = "MUTATED"
assert p.parse("<html><body>y</body></html>").find(".//body").text == "y"

# 信号
assert await_order([(0, low), (10, high)]) == ["high", "low"]   # 高优先级先执行
assert await_call_count(sender=None) == 1
```

## 4. 备选方案

| 方案 | 描述 | 结论 | 理由 |
|------|------|------|------|
| A. 只按问题条目逐行打补丁 | 在 `link_allowed` 与模块级函数各加一行 `link.url if not normalize` | 拒绝 | 两套实现继续并行，E4/E5 的语义分叉仍会复发；I1 无法保证 |
| B. 保留两套实现，只对齐常量 | 复制同一组 `_host_allowed*` 到两处 | 拒绝 | 根因是重复实现，复制只是把漂移推迟到下一次修改 |
| C. 统一 `_LinkFilter` 核心（**采纳**） | 两个公开入口都委托给同一判定核心，输入只接受 `str` | 采纳 | 一处修复覆盖 P1-05/15/16/17；判定矩阵可用参数化测试穷举 |
| D. `normalize=False` 时用原始 URL 判定 | 最小化行为变化 | 拒绝 | 判定结果会随输出格式变化；同一链接在两种模式下可能一个通过一个拒绝，违反 I2 |
| E. 判定始终用 canonical、输出按参数重建（**采纳**） | §2.1 | 采纳 | 判定与输出解耦，E3 修复，且两种模式的接受集合一致 |
| F. 模块级缓存（问题清单建议） | 把解析做成模块级纯函数缓存 | 拒绝 | 跨实例共享可变 lxml 树，且不同 `Parser` 的 `HTMLParser` 选项/大小限制不同；本阶段的修复会在阶段 4 接线时变成回归 |
| G. 保留 `lru_cache`，改为 `staticmethod` | key 不含 `self` | 拒绝 | 仍是类级全局缓存，实例配置与返回值仍共享；无法按实例清空 |
| H. 只声明「返回树只读」 | 不复制 | 拒绝 | 无法强制；E10 已证明静默污染，且用户很难定位 |
| I. 实例级 OrderedDict + 每次返回深拷贝（**采纳**） | §2.3 | 采纳 | 实例可回收（E11）、统计可断言（E9）、调用方互不干扰（E10）；解析仍只做一次 |
| J. 环境变量用单下划线分层 | `WEBANTS_DOWNLOADER_CONCURRENT_REQUESTS` | 拒绝 | 键名本身含下划线（`concurrent_requests`、`max_domain_concurrent`），无法无歧义拆分 |
| K. 环境变量用映射表 | 显式登记 `WEBANTS_X` → 路径 | 拒绝 | 新配置项需要改代码，违背配置驱动的目的；`__` 已足够表达层级 |
| L. 排序键 `(-priority, handler.__name__)` | 少一个字段 | 拒绝 | 同名函数仍会回退比较；且不是稳定顺序 |
| M. 接收器带自增序号（**采纳**） | §2.4 | 采纳 | 排序键全为 `int`，同优先级按注册顺序，行为可复现 |
| N. `_get_receivers` 按 handler 去重 | 一次 send 只执行一次 | 拒绝 | 显式注册两次应执行两次；只应修 `None` 被拼接两次的错误 |

## 5. 影响与后果

### 5.1 破坏性变更与迁移

| 变更 | 影响 | 迁移方式 |
|------|------|----------|
| 模块级过滤在无 `hosts_allow`/`extensions_allow` 时返回非空 | 依赖「默认返回空」的代码 | 若确实要拒绝全部，显式配置 allow 列表；这是 E1 的预期修复 |
| `lenient=True` 且无 `hosts_allow` 时放行 | 依赖「lenient 空配置全拒」的代码 | 无需迁移；如需拒绝，配置 `hosts_allow` |
| `normalize=True` 的返回 `Link` 已归一化 | 依赖原始 URL 字符串做去重的代码 | 去重键改为归一化后的 URL；`normalize=False` 可保留原始形式 |
| 无路径/无扩展名 URL 在配置 `extensions_allow` 时被拒 | 之前被「空扩展名恒拒」掩盖的行为被明确化 | 将无扩展名页面加入 `extensions_allow`（如 `["html", ""]` 不推荐）或改用 `extensions_deny` |
| 模块级 `scheme deny` 真正生效 | 之前 deny 配置无效 | 检查现有 `schemes_deny` 配置，避免意外拦截 |
| 模块级函数新增 `lenient` 关键字参数 | 纯新增 | 无需迁移 |
| 环境变量嵌套分隔符为 `__`，未知顶层键被忽略并告警 | 旧写法 `WEBANTS_DOWNLOADER_CONCURRENT_REQUESTS` 不再产生顶层键（本来也未生效） | 改为 `WEBANTS_DOWNLOADER__CONCURRENT_REQUESTS` |
| 非法类型的环境变量被跳过并告警 | 依赖进程启动即失败来发现配置错误的部署 | 改为查看告警日志；如需严格模式，后续可加 `Config(strict=True)`（本 ADR 不做） |
| `as_dict()` 返回深拷贝 | 通过修改返回值改配置的代码 | 改用 `set()` / `_merge_config()`；读取不受影响 |
| `Parser._cached_parse` 移除 | 直接调用私有方法的测试/代码 | 改用 `Parser.get_stats()["cache_info"]` 与公开 `parse()` |
| `Parser.parse()` 每次返回副本 | 依赖 `t1 is t2` 的代码 | 不应依赖对象身份；比较 `etree.tostring()` 或元素内容 |
| `cache_hits` 首次解析为 0；新增 `cache_misses` | 依赖旧统计值的监控/测试 | 按 §2.5 口径调整；`cache_info` 键形状保留 |
| 信号优先级方向反转（高值先执行） | 现有接收器的执行顺序 | 需要旧顺序的接收器把 `priority` 取反；`connect` 文档本来就是 `higher = earlier` |
| 同优先级按注册顺序（不再抛异常） | 之前根本无法运行 | 无需迁移 |
| `send(None)` 只执行一次 | 依赖重复执行的代码（几乎不可能是有意为之） | 无需迁移 |

### 5.2 非破坏性收益

- E1–E15 每条都有对应验收条件，阶段 3 的「提取器矩阵、Config 合并、缓存统计、信号排序全部通过」可以逐条判定，而不是靠整体感觉。
- 链接过滤只剩一个判定核心，后续新增过滤维度（如 depth、content-type）只改一处。
- 配置默认值不可污染后，`Config` 才具备被阶段 4 注入的前提；环境变量告警让配置错误在日志里可见。
- Parser 实例可被回收，长时间运行的爬虫不会因反复构造 `Parser` 而泄漏；缓存命中统计可作为后续容量规划的依据。
- 信号顺序确定后，中间件与持久化的钩子执行顺序才有可复现的测试基础（阶段 4 依赖）。

### 5.3 风险与权衡

- **每次 `parse()` 多一次深拷贝**：深拷贝远快于解析，但仍不是零成本。方案 H 用「只读约定」省掉这次拷贝，但 E10 证明静默污染更难排查；本 ADR 选择可强制、可测试的正确性。若后续压测显示大文档开销显著，另开 ADR 增加 `copy_on_read=False` 选项，而不是默认关闭。
- **判定始终使用 canonical URL**：`normalize_url` 会去掉默认端口、排序 query、去 fragment，理论上可能让某些「看起来不同」的 URL 归为同一判定结果。由于 host/scheme/extension 判定不受这些差异影响，实际行为变化应可忽略；测试矩阵要求覆盖 `normalize × allow/deny` 组合。
- **未知环境变量被忽略**：拼错变量名不会让进程失败，只会告警；若部署环境不收集日志，错误仍可能潜伏。严格模式留待后续。
- **信号优先级方向反转**：这是破坏性变更，但当前异步 `SignalManager` 尚未接入 Spider（P1-11），实际受影响面很小；趁阶段 4 之前修正成本最低。
- **模块级 `lenient` 参数增加 API 面**：与类级对齐比保持两套语义更可控；默认值与旧行为（同时 strict + lenient）不同，但旧行为在空配置下恒为空，几乎没有可依赖的合理用法。

## 6. 实施拆解

> 本次仅提交本 ADR，不改代码。

| 任务 | 内容 | 主要文件 | 依赖 | 覆盖问题 |
|------|------|----------|------|----------|
| T1 | 抽出 `_LinkFilter`（extension/host/scheme/regex 判定 + `lenient` 矩阵）；模块级 `extract_and_filter_links` 改为委托，新增 `lenient` 参数；修正模块级 `_scheme_denied` | `webants/libs/extractor.py` | — | P1-15、P1-16、P2-03（部分） |
| T2 | `FilteringLinkExtractor` 委托同一核心；`_canonical` 判定 + `normalize` 输出重建；`link_process_func` 顺序固定 | `webants/libs/extractor.py` | T1 | P1-05、P1-17 |
| T3 | `copy.deepcopy(DEFAULT_CONFIG)`；`_deep_merge`；`__` 环境变量分隔符、未知键忽略、类型守卫；`as_dict()` 深拷贝；`_validate_config` 类型判断 | `webants/libs/config.py` | — | P1-06、P2-16 |
| T4 | 移除 `_cached_parse`；实例级 `OrderedDict` + 纯哈希 key；深拷贝返回；`cache_hits/misses`、`css/xpath_cache_misses` 口径修正；`cache_info` 兼容投影；`cache_size` 参数 | `webants/parser.py` | — | P1-07、P2-17 |
| T5 | 接收器三元组 + `itertools.count`；`_get_receivers` 修 `None`；`send` 排序键 `(-priority, seq)` | `webants/libs/signals.py` | — | P1-08、P2-15 |
| T6 | 测试与文档：新增 `tests/test_config.py`、`tests/test_signals.py`；扩充 `tests/test_extractor.py` 的 4×2 lenient 矩阵与 normalize 矩阵；改写 `tests/test_parser.py` 的统计与缓存用例；同步 01/03/06/08/09 | `tests/`、`docs/` | T1–T5 | 全部 |

实施顺序：T1 → T2（共享核心必须先落地）；T3/T4/T5 可并行；T6 最后统一回归。阶段 4（P1-11 接线）必须在本 ADR 实施完成后开始，避免把错误语义带进主链路。

## 7. 验收标准

- [ ] E1：`extract_and_filter_links('<a href="/p">x</a>', base_url="http://example.com")` 返回 1 条，不再为空。
- [ ] E2：`FilteringLinkExtractor(normalize=False).extract(HTML)` 正常返回，不抛 `AttributeError`。
- [ ] E3：`normalize=True` 输出的 URL 已去掉默认端口、fragment 并排序 query；`normalize=False` 输出与 `link_process_func` 结果一致。
- [ ] E4：配置 `hosts_allow=["example.com"]` 时，`http://example.com` 与 `http://example.com/p` 都被接受。
- [ ] E5：`lenient=True` 且无 `hosts_allow` 时返回非空；`lenient × hosts_allow` 四种组合均有参数化用例。
- [ ] E6/E7：嵌套合并后顶层无 `concurrent_requests`；实例修改不影响 `DEFAULT_CONFIG`，新建实例读回默认值。
- [ ] E8：`WEBANTS_DOWNLOADER__CONCURRENT_REQUESTS=42` 使 `get("downloader.concurrent_requests") == 42`；旧单下划线写法不产生顶层键且产生告警。
- [ ] E15：嵌套键设置为非数值字符串时，`Config()` 不抛异常，保留原值并告警。
- [ ] E9：首次解析 `cache_hits == 0`、`cache_misses == 1`；同一内容第二次解析 `cache_hits == 1`。
- [ ] E10：修改第一次返回的树后，第二次同内容解析结果不受影响（返回对象不同、内容一致）。
- [ ] E11：`del parser` + `gc.collect()` 后 `weakref` 失效，无需调用 `cache_clear()`。
- [ ] 缓存容量：写入 `cache_size + 1` 个不同内容后，`currsize == maxsize`；再次写入仍保持上限。
- [ ] E12：两个 `priority=0` 接收器 `send` 不抛 `TypeError`，按注册顺序执行。
- [ ] E13：`priority=10` 的接收器先于 `priority=0` 执行。
- [ ] E14：`send(None)` 对「仅注册到 `None`」的处理器只执行一次；具体 sender + 全局注册的同一处理器仍执行两次。
- [ ] `uv run pytest -q` 全绿且离线；基线 `124 passed` 不得减少，新增用例计入总数。
- [ ] `uv run python -m isort --check .` 对修改文件无非预期新增（Windows 下不要用 `uv run isort` 入口）。
- [ ] 文档同步：03 的 Parser 缓存/提取流程、06 的双实现与缓存条目、08 §8.11/§8.13 状态、README 的能力描述与新契约一致。

## 8. 评审结论 / 未决点

**已接受（2026-11-05）**——以下结论随本 ADR 一并冻结，实施按此执行：

| # | 问题 | 结论 |
|---|------|------|
| 1 | 过滤判定用 canonical URL 还是与 `normalize` 同步 | **始终 canonical**；判定与输出解耦（I2） |
| 2 | 是否顺带修正模块级 `_scheme_denied`（P2-03 的一项） | **是**；T1 重写该函数时一并修正，P2-03 其余两项仍 Open |
| 3 | 模块级函数是否新增 `lenient` 参数 | **新增**，默认 `False`，与类对齐 |
| 4 | Parser 缓存放模块级还是实例级 | **实例级**；避免跨实例共享可变树与配置串扰 |
| 5 | 缓存命中是否返回副本 | **返回 `copy.deepcopy`**；不提供默认关闭的选项 |
| 6 | `cache_size` 是否接到 `Config` | **不接**；保留构造参数，阶段 4 接线时再统一 |
| 7 | 环境变量分隔符 | **`__`**；未知顶层键忽略并告警；单下划线保持为键名 |
| 8 | 显式重复注册的 handler 是否去重 | **不去重**；只修 `None + None` 的拼接错误 |
| 9 | 统计字段 | **新增** `cache_misses`、`css_cache_misses`、`xpath_cache_misses`；`cache_info` 保留原键形状 |
| 10 | 信号优先级方向 | **高值先执行**，破坏性变更，不提供兼容开关 |

## 9. 参考

- [08 问题与建议](../wiki/08-issues-and-recommendations.md) — P1-05、P1-06、P1-07、P1-08、P1-15、P1-16、P1-17、P2-15、P2-16、P2-17、P2-03；§8.4 簇 C；§8.11 阶段 3；§8.12 提取器/Config 条目
- [03 主链路](../wiki/03-crawler.md) — §Parser 缓存与链接过滤（`_cached_parse`、`link_allowed`）
- [01 架构](../wiki/01-architecture.md) — §1.2 运行时结构、可选层未接线、架构风险「双 Signal 体系」
- [06 依赖](../wiki/06-dependencies.md) — `Parser` 类级缓存与 `SignalManager` 风险条目
- [ADR-0003](0003-shutdown-cancellation-and-loop-quiescence.md) — §2.6 统计口径与阶段 4 接线前提
- 代码位置：`webants/libs/extractor.py:224-357,759-949`、`webants/libs/config.py:18-74,92,122-171,200-235`、`webants/parser.py:22-262`、`webants/libs/signals.py:27-159`
- 测试位置：`tests/test_extractor.py:176-196`、`tests/test_parser.py:98-149`
