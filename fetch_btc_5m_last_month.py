#!/usr/bin/env python3
"""
Dome API - 过去一个月 BTC 5分钟 方向预测市场 完整订单簿数据
输出目录: Mac 桌面 / Dome API Data / <数据名称_时间戳>
数据名称: BTC_5分钟_订单簿_过去一个月

并发版本：5 workers + side_a/side_b 并行 + 令牌桶限速
如需继续写入同一批目录，可设置环境变量 DOME_OUTPUT_TIMESTAMP
"""

import json
import os
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Any, Optional
import logging
import requests

# 过去一个月的时间范围
END_DATE = datetime.utcnow()
START_DATE = END_DATE - timedelta(days=30)
START_TIME_SEC = int(START_DATE.timestamp())
END_TIME_SEC = int(END_DATE.timestamp())
START_TIME_MS = START_TIME_SEC * 1000
END_TIME_MS = END_TIME_SEC * 1000

# 输出目录与数据名称
BASE_DIR = Path("/Users/ocean/Desktop/Dome API Data")
DATA_LABEL = "BTC_5分钟_订单簿_过去一个月"
DATA_TYPE_LABEL = "BTC订单簿"
INTERVAL_LABEL = "5M"
RUN_STARTED_AT = datetime.now().astimezone()
RUN_TIMESTAMP = os.getenv("DOME_OUTPUT_TIMESTAMP") or RUN_STARTED_AT.strftime("%Y%m%d_%H%M%S")
OUTPUT_FOLDER_NAME = f"{DATA_TYPE_LABEL}-{RUN_TIMESTAMP}-{INTERVAL_LABEL}"
OUTPUT_DIR = BASE_DIR / OUTPUT_FOLDER_NAME
LOG_FILE = BASE_DIR / "fetch_btc_5m_last_month_log.txt"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger(__name__)

API_BASE_URL = "https://api.domeapi.io/v1"
API_KEY = "642f4f5c8a11c179e997b0ee15c0a4795b1474dd"
CONCURRENCY = 5  # 并行 worker 数量

# 飞书机器人 Webhook：跑完后推送完成反馈
LARK_WEBHOOK_URL = "https://open.larksuite.com/open-apis/bot/v2/hook/b7651f44-ff95-479f-a42e-78566de0a916"


class TokenBucketRateLimiter:
    """令牌桶限速器，确保不超过 Free Tier 的 10 QPS / 100 per 10s"""

    def __init__(self, rate: float = 9.0, burst: int = 9):
        self._rate = rate
        self._burst = burst
        self._tokens = float(burst)
        self._last = time.monotonic()
        self._lock = threading.Lock()

    def acquire(self):
        while True:
            with self._lock:
                now = time.monotonic()
                elapsed = now - self._last
                self._tokens = min(self._burst, self._tokens + elapsed * self._rate)
                self._last = now
                if self._tokens >= 1.0:
                    self._tokens -= 1.0
                    return
            time.sleep(0.05)


rate_limiter = TokenBucketRateLimiter(rate=7.0, burst=5)


def send_lark_notification(data_name: str, time_start: str, time_end: str, total: int, success: int, output_path: str):
    """跑完后通过飞书 Webhook 推送反馈（飞书要求 msg_type + content）"""
    text = (
        f"【Dome API 数据拉取完成】\n\n"
        f"数据名称：{data_name}\n"
        f"时间范围：{time_start} ~ {time_end}\n"
        f"市场总数：{total}\n"
        f"成功写入：{success} 个市场\n"
        f"保存位置：{output_path}\n\n"
        f"任务已全部跑完。"
    )
    body = {
        "msg_type": "text",
        "content": {"text": text},
    }
    try:
        resp = requests.post(LARK_WEBHOOK_URL, json=body, timeout=10)
        if resp.status_code == 200 and resp.json().get("code") == 0:
            logger.info("飞书推送已发送")
        else:
            logger.warning(f"飞书推送返回: {resp.status_code} {resp.text[:200]}")
    except Exception as e:
        logger.warning(f"飞书推送失败: {e}")


def send_lark_notification_progress(data_name: str, completed: int, total: int):
    """每完成 1000 个市场时推送一次进度"""
    pct = completed / total * 100 if total else 0
    text = (
        f"【Dome API 数据拉取进度】\n\n"
        f"数据名称：{data_name}\n"
        f"已完成：{completed} / {total} 个市场（{pct:.1f}%）\n"
        f"采集仍在进行中……"
    )
    body = {"msg_type": "text", "content": {"text": text}}
    try:
        resp = requests.post(LARK_WEBHOOK_URL, json=body, timeout=10)
        if resp.status_code == 200 and resp.json().get("code") == 0:
            logger.info(f"飞书进度推送已发送（{completed}/{total}）")
        else:
            logger.warning(f"飞书进度推送返回: {resp.status_code} {resp.text[:200]}")
    except Exception as e:
        logger.warning(f"飞书进度推送失败: {e}")


def api_request(url: str, params: dict, max_retries: int = 5) -> Optional[Dict]:
    headers = {"X-API-Key": API_KEY, "Content-Type": "application/json"}
    for attempt in range(max_retries):
        rate_limiter.acquire()
        try:
            resp = requests.get(url, headers=headers, params=params, timeout=30)
            if resp.status_code == 200:
                return resp.json()
            elif resp.status_code == 429:
                wait = 2 ** (attempt + 1)
                logger.warning(f"429 限速，等待 {wait}s")
                time.sleep(wait)
            elif resp.status_code in (502, 503):
                time.sleep(5 * (attempt + 1))
            else:
                logger.error(f"API {resp.status_code}: {resp.text[:200]}")
                return None
        except (requests.exceptions.Timeout, requests.exceptions.ConnectionError):
            time.sleep(3)
    return None


def fetch_markets() -> List[Dict]:
    """优先从本地 manifest 加载市场列表，否则从 API 获取"""
    manifest_path = OUTPUT_DIR / "markets_manifest.json"
    if manifest_path.exists():
        logger.info("从本地 markets_manifest.json 加载市场列表（跳过 API 翻页）")
        with open(manifest_path, "r", encoding="utf-8") as f:
            manifest = json.load(f)
        cached = manifest.get("markets", [])
        if cached:
            logger.info(f"本地缓存命中：{len(cached)} 个市场")
            return sorted(cached, key=lambda x: x.get("start_time", 0))
        logger.warning("本地缓存为空，改用 API 获取")

    url = f"{API_BASE_URL}/polymarket/markets"
    all_markets = []
    pagination_key = None

    while True:
        params = {
            "tags": ["Up or Down", "Bitcoin"],
            "start_time": START_TIME_SEC,
            "end_time": END_TIME_SEC,
            "status": "closed",
            "limit": 100,
        }
        if pagination_key:
            params["pagination_key"] = pagination_key

        data = api_request(url, params)
        if not data:
            break

        markets = data.get("markets", [])
        for m in markets:
            slug = m.get("market_slug", "")
            if slug.startswith("btc-updown-5m"):
                all_markets.append(m)

        logger.info(f"已获取 {len(markets)} 条市场，当前 BTC 5m 累计 {len(all_markets)}")

        pagination = data.get("pagination", {})
        if not pagination.get("has_more", False):
            break
        pagination_key = pagination.get("pagination_key")
        if not pagination_key:
            break
        time.sleep(0.3)

    return sorted(all_markets, key=lambda x: x.get("start_time", 0))


def get_existing_completed_cids() -> set:
    """扫描已完整写入的市场目录，用于断点续传"""
    existing = set()
    if not OUTPUT_DIR.exists():
        return existing
    for p in OUTPUT_DIR.iterdir():
        if not p.is_dir() or not p.name.startswith("market_"):
            continue
        cid = p.name.replace("market_", "")
        if (p / "metadata.json").exists() and (p / "orderbook_side_a.json").exists() and (p / "orderbook_side_b.json").exists():
            existing.add(cid)
    return existing


def fetch_orderbook_snapshots(token_id: str, start_ms: int, end_ms: int) -> List[Dict]:
    url = f"{API_BASE_URL}/polymarket/orderbooks"
    all_snapshots = []
    pagination_key = None

    while True:
        params = {
            "token_id": token_id,
            "start_time": start_ms,
            "end_time": end_ms,
            "limit": 200,
        }
        if pagination_key:
            params["pagination_key"] = pagination_key

        data = api_request(url, params)
        if not data:
            break

        snapshots = data.get("snapshots", [])
        all_snapshots.extend(snapshots)

        pagination = data.get("pagination", {})
        if not pagination.get("has_more", False):
            break
        pagination_key = pagination.get("pagination_key")
        if not pagination_key:
            break

    return all_snapshots


def main():
    logger.info("=" * 60)
    logger.info("Dome API - 过去一个月 BTC 5分钟 订单簿数据")
    logger.info("=" * 60)
    logger.info(f"时间范围: {START_DATE.date()} ~ {END_DATE.date()}")
    logger.info(f"输出目录: {OUTPUT_DIR}")
    logger.info(f"数据名称: {DATA_LABEL}")

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    markets = fetch_markets()
    logger.info(f"共 {len(markets)} 个 BTC 5分钟 市场")

    if not markets:
        logger.warning("未获取到任何市场，请检查时间范围或 API")
        send_lark_notification(
            data_name=DATA_LABEL,
            time_start=str(START_DATE.date()),
            time_end=str(END_DATE.date()),
            total=0,
            success=0,
            output_path=str(OUTPUT_DIR),
        )
        return 0, 0

    # 保存市场列表（便于核对名称与时间）
    markets_manifest = {
        "data_name": DATA_LABEL,
        "description": "Polymarket BTC 5分钟方向预测市场完整订单簿，过去一个月",
        "time_range": {
            "start": START_TIME_SEC,
            "end": END_TIME_SEC,
            "start_readable": START_DATE.isoformat(),
            "end_readable": END_DATE.isoformat(),
        },
        "export": {
            "run_timestamp": RUN_TIMESTAMP,
            "run_started_at": RUN_STARTED_AT.isoformat(timespec="seconds"),
            "output_folder_name": OUTPUT_FOLDER_NAME,
            "data_type_label": DATA_TYPE_LABEL,
            "interval_label": INTERVAL_LABEL,
        },
        "source": "Dome API (https://docs.domeapi.io/)",
        "market_count": len(markets),
        "markets": markets,
    }
    with open(OUTPUT_DIR / "markets_manifest.json", "w", encoding="utf-8") as f:
        json.dump(markets_manifest, f, ensure_ascii=False, indent=2)

    existing_cids = get_existing_completed_cids()
    total_already = len(existing_cids)
    if total_already > 0:
        logger.info(f"断点续传：已存在 {total_already} 个市场，将跳过并继续拉取剩余")

    pending = []
    for market in markets:
        cid = market.get("condition_id", "")
        if cid not in existing_cids:
            pending.append(market)
    logger.info(f"待拉取：{len(pending)} 个市场（跳过 {total_already} 个已完成）")

    counter_lock = threading.Lock()
    progress = {"success": 0, "last_milestone": (total_already // 1000) * 1000}

    def process_one_market(market: Dict) -> bool:
        slug = market.get("market_slug", "")
        cid = market.get("condition_id", "")
        side_a = market.get("side_a", {})
        side_b = market.get("side_b", {})
        token_a = side_a.get("id")
        token_b = side_b.get("id")

        if not token_a or not token_b:
            logger.warning(f"{slug} 缺少 token_id，跳过")
            return False

        start_ts = market.get("start_time", 0) * 1000
        end_ts = market.get("end_time", 0) * 1000 or END_TIME_MS
        q_start = max(start_ts - 60_000, START_TIME_MS)
        q_end = min(end_ts + 60_000, END_TIME_MS)

        with ThreadPoolExecutor(max_workers=2) as side_pool:
            fa = side_pool.submit(fetch_orderbook_snapshots, token_a, q_start, q_end)
            fb = side_pool.submit(fetch_orderbook_snapshots, token_b, q_start, q_end)
            ob_a = fa.result()
            ob_b = fb.result()

        market_dir = OUTPUT_DIR / f"market_{cid}"
        market_dir.mkdir(parents=True, exist_ok=True)

        with open(market_dir / "metadata.json", "w", encoding="utf-8") as f:
            json.dump(market, f, ensure_ascii=False, indent=2)
        if ob_a:
            with open(market_dir / "orderbook_side_a.json", "w", encoding="utf-8") as f:
                json.dump(ob_a, f, indent=2)
        if ob_b:
            with open(market_dir / "orderbook_side_b.json", "w", encoding="utf-8") as f:
                json.dump(ob_b, f, indent=2)

        logger.info(f"{slug}  Side A: {len(ob_a)}, Side B: {len(ob_b)} 条快照")
        return True

    with ThreadPoolExecutor(max_workers=CONCURRENCY) as pool:
        futures = {pool.submit(process_one_market, m): m for m in pending}
        for future in as_completed(futures):
            market = futures[future]
            slug = market.get("market_slug", "")
            try:
                ok = future.result()
                if ok:
                    with counter_lock:
                        progress["success"] += 1
                        total_completed = total_already + progress["success"]
                        if total_completed >= progress["last_milestone"] + 1000:
                            send_lark_notification_progress(
                                data_name=DATA_LABEL,
                                completed=total_completed,
                                total=len(markets),
                            )
                            progress["last_milestone"] = (total_completed // 1000) * 1000
                        if progress["success"] % 50 == 0:
                            logger.info(f"进度：本轮新增 {progress['success']}，累计 {total_completed}/{len(markets)}")
            except Exception as e:
                logger.exception(f"{slug} 处理异常: {e}")

    total_completed = total_already + progress["success"]

    # 数据名称与说明文件
    readme = f"""# {DATA_LABEL}

## 数据名称
{DATA_LABEL}

## 说明
- 内容：Polymarket 上 **BTC 5分钟方向预测** 市场的完整订单簿历史快照（Fully Order Book）
- 时间范围：**过去一个月**（{START_DATE.date()} ~ {END_DATE.date()}）
- 导出时间：**{RUN_STARTED_AT.isoformat(timespec="seconds")}**
- 数据来源：Dome API (https://docs.domeapi.io/)
- 市场数量：{len(markets)}
- 成功写入订单簿的市场数：{total_completed}

## 目录结构
- 导出目录：`{OUTPUT_FOLDER_NAME}`
- 命名规则：`{DATA_TYPE_LABEL}-时间戳-{INTERVAL_LABEL}`
- `markets_manifest.json`：市场列表与时间范围、数据名称等元信息
- `market_<condition_id>/`：每个市场一个文件夹
  - `metadata.json`：市场基本信息（标题、时间、token 等）
  - `orderbook_side_a.json`：Up 方向订单簿快照数组
  - `orderbook_side_b.json`：Down 方向订单簿快照数组

## 时间戳
- 订单簿快照中 timestamp、indexedAt 均为毫秒级 Unix 时间戳
"""
    with open(OUTPUT_DIR / "README_数据说明.md", "w", encoding="utf-8") as f:
        f.write(readme)

    logger.info("=" * 60)
    logger.info(f"完成。本次新写入 {progress['success']}，累计 {total_completed}/{len(markets)} 个市场")
    logger.info(f"数据保存位置: {OUTPUT_DIR}")
    logger.info(f"数据名称: {DATA_LABEL}")
    logger.info("=" * 60)

    # 跑完后通过飞书 Webhook 推送反馈
    send_lark_notification(
        data_name=DATA_LABEL,
        time_start=str(START_DATE.date()),
        time_end=str(END_DATE.date()),
        total=len(markets),
        success=total_completed,
        output_path=str(OUTPUT_DIR),
    )

    return total_completed, len(markets)


if __name__ == "__main__":
    while True:
        try:
            total_done, total = main()
            if total > 0 and total_done >= total:
                logger.info("全部拉取完成，脚本退出")
                break
            if total == 0:
                break
            logger.warning(f"本轮完成 {total_done}/{total}，60 秒后重试未完成的……")
            time.sleep(60)
        except KeyboardInterrupt:
            logger.info("用户中断，退出")
            break
        except Exception as e:
            logger.exception(f"main() 异常: {e}，60 秒后重试")
            time.sleep(60)
