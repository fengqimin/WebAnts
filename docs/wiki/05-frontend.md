# 05 前端

## 5.1 结论：本仓库没有前端

WebAnts 是一个纯 Python 库，不包含浏览器端代码，也没有 Web 服务。仓库中不存在以下内容：

- `package.json`、`vite.config.*`、`webpack.config.*`、`tsconfig.json`；
- `templates/`、`static/`、`public/`、`src/` 等前端目录；
- HTML 模板渲染、路由、组件、状态管理、API 客户端；
- 任何监听端口的代码（`FastAPI`、`Flask`、`uvicorn`、`aiohttp.web` 均未出现在依赖或 import 中）。

因此没有“路由表”“组件树”“页面状态表”可写；本章只说明仓库里唯一的可执行交互面。

## 5.2 唯一的交互入口：示例脚本

| 项目 | 内容 | 证据 |
|------|------|------|
| 文件 | `example/use_item.py` | — |
| 形态 | 模块级脚本，非函数、非 CLI | `example/use_item.py:403-418` |
| 输入 | 内嵌的一大段 Python 文档 HTML 字符串 | `example/use_item.py:14-397` |
| 行为 | 定义 `P` 类，两个描述符字段 `links`/`title`，实例化后 `print` | `example/use_item.py:403-418` |
| 输出 | `Link` 对象列表与标题文本，打印到 stdout | `example/use_item.py:417-418` |
| 副作用 | **import 即执行**（没有 `if __name__ == "__main__"`） | `example/use_item.py:415-418` |

示例声明的两个字段：

| 字段 | 类型 | 选择器 | 说明 |
|------|------|--------|------|
| `links` | `LinkItemDescriptor` | `table.contentstable a.biglink` | `base_url=https://docs.python.org/3/index.html`，`many=True` |
| `title` | `TextItemDescriptor` | `title` | `many=False` |

来源：`example/use_item.py:403-409`。

## 5.3 对使用方的建议

- 如果需要一个可视化界面，应在 WebAnts 之外单独实现：WebAnts 只负责抓取，结果通过 `Result.to_dict()` 或 `PersisterManager` 输出，再由前端/报表工具消费。
- 如果只是想验证提取规则，可参考 `example/use_item.py` 的写法，但建议把它改成 `main()` 函数并加 `if __name__ == "__main__"`，避免 import 副作用。
- 当前仓库没有任何前端构建步骤，[07 运行与运维](07-operations.md) 中的命令全部是 Python 侧命令。
