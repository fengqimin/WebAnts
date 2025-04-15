# 概述

一个用`httpx` 实现的简单的异步网络爬虫框架。

## 架构

```mermaid
graph TD
    A[Spider] --> B[Scheduler]
    B --> C[Downloader]
    C --> D[Parser]
    D --> B
    D --> A

 ```

## 主要数据流

1. Spider 初始化请求 -> Scheduler
2. Scheduler 调度请求 -> Downloader
3. Downloader 获取响应 -> Parser
4. Parser 解析数据生成新请求 -> Scheduler
5. Parser 解析数据生成结果 -> Spider
