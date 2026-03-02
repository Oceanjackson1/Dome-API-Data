# Dome API Data

通过 [Dome API](https://docs.domeapi.io/) 批量拉取 [Polymarket](https://polymarket.com/) 上 **BTC 5 分钟方向预测市场** 的历史数据（订单簿 + 成交记录），用于量化研究与数据分析。

---

## 项目概览

| 数据类型 | 脚本 | API 端点 | 时间范围 |
|---------|------|---------|---------|
| 订单簿（Order Book） | `fetch_btc_5m_last_month.py` | `/v1/polymarket/orderbooks` | 过去 30 天（动态） |
| 订单簿（Order Book） | `fetch_btc_5m_multi_api.py` | `/v1/polymarket/orderbooks` | 2026-01-01 ~ 2026-03-01 |
| 成交记录（Trades） | `fetch_btc_5m_trades_multi_api.py` | `/v1/polymarket/orders` | 2026-02-12 ~ 2026-03-01 |

---

## 脚本说明

### 1. `fetch_btc_5m_last_month.py` — 订单簿（单 Key 版）

- **功能**：拉取过去 30 天 BTC 5 分钟方向预测市场的完整订单簿快照
- **并发**：5 workers + side_a/side_b 并行拉取
- **限速**：令牌桶限速（7 QPS，burst 5）
- **输出**：`BTC订单簿-<YYYYMMDD_HHMMSS>-5M/` 目录

### 2. `fetch_btc_5m_multi_api.py` — 订单簿（多 Key 并发版）

- **功能**：拉取 2026 Q1（01-01 ~ 03-01）BTC 5 分钟市场的完整订单簿快照
- **并发**：6 个 API Key 轮询，30 workers 并发
- **限速**：每个 Key 独立令牌桶限速（7 QPS，burst 5）
- **断点续传**：扫描已完成的市场目录，自动跳过已拉取的市场
- **飞书通知**：启动/首个落盘/第 100 个/每 1000 个/完成时推送
- **输出**：`BTC-Poly-FullyOrderBook-<YYYYMMDD_HHMMSS>-5M/` 目录
- **每市场文件**：`metadata.json` + `orderbook_side_a.json` + `orderbook_side_b.json`

### 3. `fetch_btc_5m_trades_multi_api.py` — 成交记录（多 Key 并发版）

- **功能**：拉取 2026-02-12 ~ 2026-03-01 BTC 5 分钟市场的全部成交记录
- **并发**：6 个 API Key 轮询，30 workers 并发
- **API 端点**：`GET /v1/polymarket/orders`，通过 `condition_id` 查询（一次请求包含 Up/Down 两侧）
- **限速**：每个 Key 独立令牌桶限速（7 QPS，burst 5）
- **断点续传**：扫描已完成的市场目录，自动跳过已拉取的市场
- **去重**：按 `(tx_hash, log_index)` 去重，按 `(timestamp, log_index)` 排序
- **飞书通知**：启动/首个落盘/第 100 个/每 1000 个/完成时推送
- **输出**：`BTC-Poly-5M-20260212-20260301-Trade/` 目录
- **每市场文件**：`metadata.json` + `trades.json`

---

## Dome API 端点

所有脚本使用 Dome API v1，基础 URL：`https://api.domeapi.io/v1`

### 市场列表

```
GET /polymarket/markets
```

| 参数 | 类型 | 说明 |
|------|------|------|
| `tags` | array | 标签筛选，如 `["Up or Down", "Bitcoin"]` |
| `start_time` | int | 起始时间（Unix 秒） |
| `end_time` | int | 结束时间（Unix 秒） |
| `status` | string | 市场状态，如 `closed` |
| `limit` | int | 每页数量（最大 100） |
| `pagination_key` | string | 分页游标 |

### 订单簿历史

```
GET /polymarket/orderbooks
```

| 参数 | 类型 | 说明 |
|------|------|------|
| `token_id` | string | Token 标识 |
| `start_time` | int | 起始时间（Unix **毫秒**） |
| `end_time` | int | 结束时间（Unix **毫秒**） |
| `limit` | int | 每页数量（最大 200） |
| `pagination_key` | string | 分页游标 |

### 成交记录

```
GET /polymarket/orders
```

| 参数 | 类型 | 说明 |
|------|------|------|
| `condition_id` | string | 市场 Condition ID |
| `start_time` | int | 起始时间（Unix **秒**） |
| `end_time` | int | 结束时间（Unix **秒**） |
| `limit` | int | 每页数量（最大 1000） |
| `pagination_key` | string | 分页游标 |

> 注意：`condition_id`、`token_id`、`market_slug` 三者只能指定一个。

---

## 输出数据结构

### 订单簿数据

```
market_<YYYYMMDD_HHMMSS_UTC>/
├── metadata.json              # 市场元数据（标题、时间、token_id 等）
├── orderbook_side_a.json      # Up 方向订单簿快照数组
└── orderbook_side_b.json      # Down 方向订单簿快照数组
```

**订单簿快照字段**：`timestamp`、`indexedAt`、`bids[]`、`asks[]`、`hash`、`assetId`、`market`、`tickSize`、`minOrderSize`

### 成交记录数据

```
market_<YYYYMMDD_HHMMSS_UTC>/
├── metadata.json              # 市场元数据（标题、时间、token_id 等）
└── trades.json                # 全部成交记录数组（含 Up/Down 两侧）
```

**成交记录字段**：

| 字段 | 说明 |
|------|------|
| `token_id` | Token 标识 |
| `token_label` | 方向标签（Up / Down） |
| `side` | 买卖方向（BUY / SELL） |
| `price` | 成交价格 |
| `shares` / `shares_normalized` | 成交份额 |
| `tx_hash` | 链上交易哈希 |
| `block_number` / `log_index` | 区块号与日志索引 |
| `timestamp` | 成交时间（Unix 秒级时间戳） |
| `user` / `taker` | 交易双方钱包地址 |
| `order_hash` | 订单哈希 |

---

## 使用方法

### 环境要求

- Python 3.8+
- 依赖：`requests`

```bash
pip install requests
```

### 运行脚本

```bash
# 订单簿 - 单 Key 版（过去 30 天）
python3 fetch_btc_5m_last_month.py

# 订单簿 - 多 Key 并发版（2026 Q1）
python3 fetch_btc_5m_multi_api.py

# 成交记录 - 多 Key 并发版（2026-02-12 ~ 2026-03-01）
python3 fetch_btc_5m_trades_multi_api.py
```

### 后台运行

```bash
nohup python3 fetch_btc_5m_trades_multi_api.py > nohup_trades.out 2>&1 &
```

### 断点续传

设置环境变量 `DOME_OUTPUT_TIMESTAMP` 可继续写入同一批输出目录：

```bash
DOME_OUTPUT_TIMESTAMP=20260302_225500 python3 fetch_btc_5m_trades_multi_api.py
```

### 强制重抓指定市场

将需要重新拉取的 `condition_id` 写入文本文件，每行一个，然后设置环境变量：

```bash
DOME_FORCE_REFRESH_CIDS_FILE=refresh_list.txt python3 fetch_btc_5m_trades_multi_api.py
```

---

## 核心机制

### 多 Key 并发限速

- 6 个 API Key 轮询分配请求
- 每个 Key 独立的令牌桶限速器（7 QPS，burst 5）
- 30 个并发 worker 线程

### 断点续传

- 启动时扫描输出目录中已完成的市场文件夹
- 通过检查必要文件是否存在判断完成状态
- 写入前加锁并二次检查，防止多 worker 重复写入

### 飞书通知

通过 Lark Webhook 推送任务进度：
- 任务启动时
- 首个市场文件夹落盘时
- 第 100 个市场完成时
- 之后每 1000 个市场推送进度
- 全部完成时

### 错误重试

- API 请求自动重试（最多 5 次）
- 429 限速：指数退避等待
- 502/503 网关错误：线性退避等待
- 超时/连接错误：3 秒后重试
- 单个市场拉取失败：最多 3 次重试

---

## API Key 配置

脚本中 API Key 以明文存储（仅限本地开发使用）。生产环境建议通过环境变量注入：

```python
API_KEYS = os.getenv("DOME_API_KEYS", "").split(",")
```

获取 API Key：前往 [Dome API](https://docs.domeapi.io/) 注册。

---

## 相关链接

- [Dome API 文档](https://docs.domeapi.io/)
- [Dome API Discord](https://discord.gg/fKAbjNAbkt)
- [Polymarket](https://polymarket.com/)
