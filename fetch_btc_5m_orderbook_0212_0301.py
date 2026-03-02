#!/usr/bin/env python3
"""
Dome API - BTC 5M Fully Order Book（多 API Key 并发版）

- 6 个 API Key 轮询，每 Key 独立限速，避免重复拉取同一市场
- 时间范围：2026-01-01 ～ 2026-03-01（UTC，固定周期）
- 输出根目录: Mac 桌面 / BTC-Poly-5M Data-Q1
- 数据子目录命名: BTC-Poly-FullyOrderBook-<时间戳>-5M
- 断点续传：仅拉取尚未完成的市场，写入前再次检查避免重复
- 如需继续写入同一批目录，可设置环境变量 DOME_OUTPUT_TIMESTAMP
- 启动时 + 每 1000 个市场推送 Lark
"""

import json
import os
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Tuple
import logging
import requests

# 固定时间周期：2026-02-12 00:00:00 UTC ～ 2026-03-01 23:59:59 UTC
START_DATE = datetime(2026, 2, 12, 0, 0, 0, tzinfo=timezone.utc)
END_DATE = datetime(2026, 3, 1, 23, 59, 59, tzinfo=timezone.utc)
START_TIME_SEC = int(START_DATE.timestamp())
END_TIME_SEC = int(END_DATE.timestamp())
START_TIME_MS = START_TIME_SEC * 1000
END_TIME_MS = END_TIME_SEC * 1000
INTERVAL_SECONDS = 5 * 60

OUTPUT_ROOT_DIR = Path("/Users/ocean/Desktop/桌面 - Ocean的MacBook Pro")
DATA_LABEL = "BTC Polymarket Fully Order Book 5M (2026-02-12 to 2026-03-01)"
DATA_TYPE_LABEL = "BTC-Poly-FullyOrderBook"
INTERVAL_LABEL = "5M"
MARKET_SLUG_PREFIX = "btc-updown-5m"
MARKET_DISPLAY_LABEL = "BTC 5M"
RUN_STARTED_AT = datetime.now().astimezone()
RUN_TIMESTAMP = os.getenv("DOME_OUTPUT_TIMESTAMP") or RUN_STARTED_AT.strftime("%Y%m%d_%H%M%S")
OUTPUT_FOLDER_NAME = "BTC-5M-FullyOrderBook-20260212-20260301"
OUTPUT_DIR = OUTPUT_ROOT_DIR / OUTPUT_FOLDER_NAME
LOG_FILE = OUTPUT_ROOT_DIR / "fetch_btc_orderbook_0212_0301_log.txt"
FORCE_REFRESH_CIDS_FILE = os.getenv("DOME_FORCE_REFRESH_CIDS_FILE", "").strip()

OUTPUT_ROOT_DIR.mkdir(parents=True, exist_ok=True)

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
# 6 个 API Key 轮询使用，不重复拉取同一市场
API_KEYS = [
    "642f4f5c8a11c179e997b0ee15c0a4795b1474dd",
    "aafc0f70ecaa32f3db1645a2024ab90c0d77526b",
    "0da7703a7b02171f28bc0565f6b92f1f84014a1b",
    "3488639103ead5fd2042436b63bca75dd0b4a441",
    "0ef6e5a21f8fd330199bc7b43b77d73b05ee6b74",
    "86670cfbb8f250345b71b0d099b0a6d4427f5112",
]
CONCURRENCY = 30  # 6 Keys × 5 ≈ 30 workers
MARKET_FETCH_RETRIES = 3

LARK_WEBHOOK_URL = "https://open.larksuite.com/open-apis/bot/v2/hook/b7651f44-ff95-479f-a42e-78566de0a916"


class TokenBucketRateLimiter:
    def __init__(self, rate: float = 7.0, burst: int = 5):
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


# 每个 Key 一个限速器，轮询分配请求
_rate_limiters = [TokenBucketRateLimiter(rate=7.0, burst=5) for _ in range(len(API_KEYS))]
_key_index = 0
_key_lock = threading.Lock()


def _next_key_and_limiter() -> Tuple[str, TokenBucketRateLimiter]:
    global _key_index
    with _key_lock:
        i = _key_index % len(API_KEYS)
        _key_index += 1
    return API_KEYS[i], _rate_limiters[i]


def send_lark_notification_start(
    data_name: str, time_start: str, time_end: str, total: int, pending: int, output_path: str
):
    """任务启动时推送"""
    completed = max(total - pending, 0)
    text = (
        f"【Dome API 数据拉取已启动】\n\n"
        f"数据名称：{data_name}\n"
        f"时间范围：{time_start} ~ {time_end}\n"
        f"市场总数：{total}\n"
        f"已完成累计：{completed}\n"
        f"剩余待跑：{pending}\n"
        f"保存位置：{output_path}\n\n"
        f"第 100 个市场完成时推送一次，之后每完成 1000 个市场推送一次。"
    )
    body = {"msg_type": "text", "content": {"text": text}}
    try:
        resp = requests.post(LARK_WEBHOOK_URL, json=body, timeout=10)
        if resp.status_code == 200 and resp.json().get("code") == 0:
            logger.info("飞书启动推送已发送")
        else:
            logger.warning(f"飞书启动推送返回: {resp.status_code} {resp.text[:200]}")
    except Exception as e:
        logger.warning(f"飞书启动推送失败: {e}")


def send_lark_notification(
    data_name: str,
    time_start: str,
    time_end: str,
    total: int,
    success: int,
    output_path: str,
    skipped: int = 0,
    new_written: int = 0,
):
    remaining = max(total - success, 0)
    text = (
        f"【Dome API 数据拉取完成】\n\n"
        f"数据名称：{data_name}\n"
        f"时间范围：{time_start} ~ {time_end}\n"
        f"市场总数：{total}\n"
        f"已完成累计：{success}\n"
        f"本轮新增：{new_written}\n"
        f"已跳过历史：{skipped}\n"
        f"剩余待跑：{remaining}\n"
        f"保存位置：{output_path}\n\n"
        f"任务已全部跑完。（多 Key 并发版）"
    )
    body = {"msg_type": "text", "content": {"text": text}}
    try:
        resp = requests.post(LARK_WEBHOOK_URL, json=body, timeout=10)
        if resp.status_code == 200 and resp.json().get("code") == 0:
            logger.info("飞书推送已发送")
        else:
            logger.warning(f"飞书推送返回: {resp.status_code} {resp.text[:200]}")
    except Exception as e:
        logger.warning(f"飞书推送失败: {e}")


def send_lark_notification_progress(
    data_name: str, completed: int, total: int, skipped: int = 0, milestone_label: str = "进度"
):
    pct = completed / total * 100 if total else 0
    remaining = max(total - completed, 0)
    text = (
        f"【Dome API 数据拉取{milestone_label}】\n\n"
        f"数据名称：{data_name}\n"
        f"市场总数：{total}\n"
        f"已完成累计：{completed} / {total}（{pct:.1f}%）\n"
        f"已跳过历史：{skipped}\n"
        f"剩余待跑：{remaining}\n"
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


def send_lark_notification_first_market(
    data_name: str, market_slug: str, output_path: str, completed: int, total: int, skipped: int = 0
):
    remaining = max(total - completed, 0)
    text = (
        f"【Dome API 首个市场文件夹已落盘】\n\n"
        f"数据名称：{data_name}\n"
        f"首个落盘市场：{market_slug}\n"
        f"市场总数：{total}\n"
        f"已完成累计：{completed}\n"
        f"已跳过历史：{skipped}\n"
        f"剩余待跑：{remaining}\n"
        f"保存位置：{output_path}\n\n"
        f"抓取任务仍在继续。"
    )
    body = {"msg_type": "text", "content": {"text": text}}
    try:
        resp = requests.post(LARK_WEBHOOK_URL, json=body, timeout=10)
        if resp.status_code == 200 and resp.json().get("code") == 0:
            logger.info(f"飞书首个市场落盘推送已发送（{market_slug}）")
        else:
            logger.warning(f"飞书首个市场落盘推送返回: {resp.status_code} {resp.text[:200]}")
    except Exception as e:
        logger.warning(f"飞书首个市场落盘推送失败: {e}")


def api_request(url: str, params: dict, max_retries: int = 5) -> Optional[Dict]:
    """使用轮询到的 API Key 发起请求，该 Key 对应限速器 acquire"""
    for attempt in range(max_retries):
        api_key, limiter = _next_key_and_limiter()
        limiter.acquire()
        headers = {"X-API-Key": api_key, "Content-Type": "application/json"}
        try:
            resp = requests.get(url, headers=headers, params=params, timeout=30)
            if resp.status_code == 200:
                return resp.json()
            elif resp.status_code == 429:
                wait = 2 ** (attempt + 1)
                logger.warning(f"429 限速，等待 {wait}s")
                time.sleep(wait)
            elif resp.status_code in (502, 503):
                wait = 5 * (attempt + 1)
                logger.warning(f"{resp.status_code} 网关错误，等待 {wait}s 后重试")
                time.sleep(wait)
            else:
                logger.error(f"API {resp.status_code}: {resp.text[:200]}")
                return None
        except (requests.exceptions.Timeout, requests.exceptions.ConnectionError):
            logger.warning(f"请求超时或连接错误，第 {attempt + 1} 次重试")
            time.sleep(3)
    return None


def fetch_markets() -> List[Dict]:
    """优先从本地 manifest 加载（仅当时间范围一致时），避免重复请求"""
    manifest_path = OUTPUT_DIR / "markets_manifest.json"
    if manifest_path.exists():
        with open(manifest_path, "r", encoding="utf-8") as f:
            manifest = json.load(f)
        tr = manifest.get("time_range", {})
        if tr.get("start") == START_TIME_SEC and tr.get("end") == END_TIME_SEC:
            cached = manifest.get("markets", [])
            if cached:
                logger.info("从本地 markets_manifest.json 加载市场列表（时间范围一致，跳过 API 翻页）")
                logger.info(f"本地缓存命中：{len(cached)} 个市场")
                return sorted(cached, key=lambda x: x.get("start_time", 0))
        else:
            logger.info("manifest 时间范围与本次不一致（本次为 2026-01-01～2026-03-01 UTC），从 API 拉取市场列表")

    url = f"{API_BASE_URL}/polymarket/markets"
    all_markets = []
    seen_market_ids = set()
    seen_page_signatures = set()
    seen_pagination_keys = set()
    stale_pages = 0
    max_stale_pages = 20
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
            raise RuntimeError("拉取市场列表失败，稍后重试")
        page_markets = data.get("markets", [])
        page_signature = tuple(
            (
                m.get("condition_id") or "",
                m.get("market_slug") or "",
                m.get("start_time") or 0,
            )
            for m in page_markets
        )
        if page_signature in seen_page_signatures:
            logger.warning("市场列表分页出现重复页面，提前停止翻页以避免死循环")
            break
        seen_page_signatures.add(page_signature)
        logger.info(f"市场列表翻页返回 {len(page_markets)} 条")
        added_this_page = 0
        for m in page_markets:
            if (m.get("market_slug") or "").startswith(MARKET_SLUG_PREFIX):
                market_id = m.get("condition_id") or m.get("market_slug")
                if market_id in seen_market_ids:
                    continue
                seen_market_ids.add(market_id)
                all_markets.append(m)
                added_this_page += 1
        logger.info(f"本页新增 {added_this_page} 个，符合 {MARKET_DISPLAY_LABEL} 条件的市场累计 {len(all_markets)} 个")
        if added_this_page == 0:
            stale_pages += 1
            if stale_pages >= max_stale_pages:
                logger.warning(f"连续 {stale_pages} 页未新增目标市场，提前停止翻页")
                break
        else:
            stale_pages = 0
        pagination = data.get("pagination", {})
        next_pagination_key = pagination.get("pagination_key")
        if not pagination.get("has_more") or not next_pagination_key:
            break
        if next_pagination_key in seen_pagination_keys:
            logger.warning("市场列表 pagination_key 重复，提前停止翻页以避免死循环")
            break
        seen_pagination_keys.add(next_pagination_key)
        pagination_key = next_pagination_key
        time.sleep(0.3)
    return sorted(all_markets, key=lambda x: x.get("start_time", 0))


def get_market_interval_start_sec(market: Dict) -> int:
    slug = market.get("market_slug") or ""
    slug_tail = slug.rsplit("-", 1)[-1]
    if slug_tail.isdigit():
        return int(slug_tail)
    end_time = int(market.get("end_time") or 0)
    if end_time:
        return max(end_time - INTERVAL_SECONDS, 0)
    return int(market.get("start_time") or 0)


def format_market_time_label(market: Dict) -> str:
    interval_start_sec = get_market_interval_start_sec(market)
    if interval_start_sec <= 0:
        return "unknown_time_UTC"
    return datetime.fromtimestamp(interval_start_sec, timezone.utc).strftime("%Y%m%d_%H%M%S_UTC")


def get_market_dir_name(market: Dict) -> str:
    return f"market_{format_market_time_label(market)}"


def read_market_condition_id(market_dir: Path) -> str:
    metadata_path = market_dir / "metadata.json"
    if metadata_path.exists():
        try:
            with open(metadata_path, "r", encoding="utf-8") as f:
                metadata = json.load(f)
            cid = metadata.get("condition_id")
            if cid:
                return cid
        except Exception:
            pass
    if market_dir.name.startswith("market_0x"):
        return market_dir.name.replace("market_", "", 1)
    return ""


def is_market_dir_complete(market_dir: Path) -> bool:
    return (
        (market_dir / "metadata.json").exists()
        and (market_dir / "orderbook_side_a.json").exists()
        and (market_dir / "orderbook_side_b.json").exists()
    )


def load_force_refresh_cids() -> set:
    if not FORCE_REFRESH_CIDS_FILE:
        return set()
    path = Path(FORCE_REFRESH_CIDS_FILE)
    if not path.exists():
        return set()
    return {line.strip() for line in path.read_text(encoding="utf-8").splitlines() if line.strip()}


def persist_force_refresh_cids(force_refresh_cids: set):
    if not FORCE_REFRESH_CIDS_FILE:
        return
    path = Path(FORCE_REFRESH_CIDS_FILE)
    if force_refresh_cids:
        path.write_text("\n".join(sorted(force_refresh_cids)) + "\n", encoding="utf-8")
    elif path.exists():
        path.unlink()


def get_existing_completed_market_dirs() -> Dict[str, Path]:
    """扫描已完整写入的市场目录，用于断点续传，确保不重复拉取"""
    existing: Dict[str, Path] = {}
    if not OUTPUT_DIR.exists():
        return existing
    for p in OUTPUT_DIR.iterdir():
        if not p.is_dir() or not p.name.startswith("market_"):
            continue
        cid = read_market_condition_id(p)
        if cid and is_market_dir_complete(p):
            existing[cid] = p
    return existing


def resolve_market_dir(market: Dict, cid: str, existing_market_dirs: Dict[str, Path]) -> Path:
    current_dir = existing_market_dirs.get(cid)
    if current_dir:
        return current_dir

    base_name = get_market_dir_name(market)
    candidate = OUTPUT_DIR / base_name
    if not candidate.exists():
        return candidate

    existing_cid = read_market_condition_id(candidate)
    if not existing_cid or existing_cid == cid:
        return candidate

    suffix = 2
    while True:
        candidate = OUTPUT_DIR / f"{base_name}_dup{suffix}"
        if not candidate.exists():
            return candidate
        existing_cid = read_market_condition_id(candidate)
        if not existing_cid or existing_cid == cid:
            return candidate
        suffix += 1


def snapshot_identity_key(snapshot: Dict) -> Tuple:
    return (
        int(snapshot.get("timestamp") or 0),
        int(snapshot.get("indexedAt") or 0),
        str(snapshot.get("hash") or ""),
        str(snapshot.get("assetId") or ""),
        str(snapshot.get("market") or ""),
    )


def normalize_snapshots(snapshots: List[Dict]) -> List[Dict]:
    deduped: Dict[Tuple, Dict] = {}
    for snapshot in snapshots:
        deduped.setdefault(snapshot_identity_key(snapshot), snapshot)
    return [deduped[key] for key in sorted(deduped)]


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
            raise RuntimeError(f"订单簿分页请求失败，token_id={token_id}")
        all_snapshots.extend(data.get("snapshots", []))
        pagination = data.get("pagination", {})
        if not pagination.get("has_more") or not pagination.get("pagination_key"):
            break
        pagination_key = pagination.get("pagination_key")
    return normalize_snapshots(all_snapshots)


# 写入前加锁并再次检查，避免多 worker 重复写入同一市场
_write_lock = threading.Lock()


def main():
    logger.info("=" * 60)
    logger.info("Dome API - BTC 5M Fully Order Book（多 API Key 并发）")
    logger.info("=" * 60)
    logger.info(f"时间范围(UTC): {START_DATE.isoformat()} ~ {END_DATE.isoformat()}")
    logger.info(f"输出根目录: {OUTPUT_ROOT_DIR}")
    logger.info(f"输出目录: {OUTPUT_DIR}")
    logger.info(f"API Keys: {len(API_KEYS)} 个, Workers: {CONCURRENCY}")

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    markets = fetch_markets()
    logger.info(f"共 {len(markets)} 个 {MARKET_DISPLAY_LABEL} 市场")

    if not markets:
        logger.warning("未获取到任何市场")
        send_lark_notification(
            data_name=DATA_LABEL,
            time_start=str(START_DATE.date()),
            time_end=str(END_DATE.date()),
            total=0,
            success=0,
            output_path=str(OUTPUT_DIR),
            skipped=0,
            new_written=0,
        )
        return 0, 0

    # 写入 manifest（当前时间范围与市场列表，便于下次同范围用缓存、不重复拉列表）
    manifest_path = OUTPUT_DIR / "markets_manifest.json"
    manifest = {
        "data_name": DATA_LABEL,
        "description": "Polymarket BTC 5M direction markets fully order book snapshots, 2026-01-01 to 2026-03-01 UTC",
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
            "output_root_dir": str(OUTPUT_ROOT_DIR),
        },
        "source": "Dome API (https://docs.domeapi.io/)",
        "market_count": len(markets),
        "markets": markets,
    }
    with open(manifest_path, "w", encoding="utf-8") as f:
        json.dump(manifest, f, ensure_ascii=False, indent=2)

    existing_market_dirs = get_existing_completed_market_dirs()
    existing_cids = set(existing_market_dirs)
    force_refresh_cids = load_force_refresh_cids() & {m.get("condition_id", "") for m in markets}
    if force_refresh_cids:
        logger.info(f"强制重抓：{len(force_refresh_cids)} 个市场将在本轮重新拉取并覆盖")
    effective_existing_cids = existing_cids - force_refresh_cids
    total_already = len(effective_existing_cids)
    if total_already > 0:
        logger.info(f"断点续传：已存在 {total_already} 个市场，将跳过，不重复拉取")

    pending = [m for m in markets if m.get("condition_id", "") not in effective_existing_cids]
    logger.info(f"待拉取：{len(pending)} 个市场（跳过 {total_already} 个已完成）")

    # 启动时推送到 Lark
    send_lark_notification_start(
        data_name=DATA_LABEL,
        time_start=str(START_DATE.date()),
        time_end=str(END_DATE.date()),
        total=len(markets),
        pending=len(pending),
        output_path=str(OUTPUT_DIR),
    )

    progress = {
        "success": 0,
        "hundred_notified": total_already >= 100,
        "last_milestone": (total_already // 1000) * 1000,
        "first_notified": total_already > 0,
    }
    progress_lock = threading.Lock()

    def process_one_market(market: Dict) -> bool:
        slug = market.get("market_slug", "")
        cid = market.get("condition_id", "")
        if not cid:
            return False
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

        last_error = None
        for attempt in range(MARKET_FETCH_RETRIES):
            try:
                with ThreadPoolExecutor(max_workers=2) as side_pool:
                    fa = side_pool.submit(fetch_orderbook_snapshots, token_a, q_start, q_end)
                    fb = side_pool.submit(fetch_orderbook_snapshots, token_b, q_start, q_end)
                    ob_a = fa.result()
                    ob_b = fb.result()
                break
            except Exception as e:
                last_error = e
                wait = 3 * (attempt + 1)
                logger.warning(f"{slug} 拉取失败，第 {attempt + 1} 次重试，等待 {wait}s：{e}")
                time.sleep(wait)
        else:
            raise RuntimeError(f"{slug} 多次重试后仍失败：{last_error}")

        # 写入前加锁并再次检查，避免重复写入
        with _write_lock:
            existing_market_dir = existing_market_dirs.get(cid)
            should_force_refresh = cid in force_refresh_cids
            if existing_market_dir and is_market_dir_complete(existing_market_dir) and not should_force_refresh:
                logger.debug(f"{slug} 已存在，跳过写入（防重复）")
                return True
            market_dir = resolve_market_dir(market, cid, existing_market_dirs)
            market_dir.mkdir(parents=True, exist_ok=True)
            with open(market_dir / "metadata.json", "w", encoding="utf-8") as f:
                json.dump(market, f, ensure_ascii=False, indent=2)
            with open(market_dir / "orderbook_side_a.json", "w", encoding="utf-8") as f:
                json.dump(ob_a, f, indent=2)
            with open(market_dir / "orderbook_side_b.json", "w", encoding="utf-8") as f:
                json.dump(ob_b, f, indent=2)
            existing_market_dirs[cid] = market_dir
            if should_force_refresh:
                force_refresh_cids.discard(cid)
                persist_force_refresh_cids(force_refresh_cids)
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
                    with progress_lock:
                        progress["success"] += 1
                        total_completed = total_already + progress["success"]
                        if not progress["first_notified"]:
                            send_lark_notification_first_market(
                                data_name=DATA_LABEL,
                                market_slug=slug or "unknown",
                                output_path=str(OUTPUT_DIR),
                                completed=total_completed,
                                total=len(markets),
                                skipped=total_already,
                            )
                            progress["first_notified"] = True
                        if not progress["hundred_notified"] and total_completed >= 100:
                            send_lark_notification_progress(
                                data_name=DATA_LABEL,
                                completed=total_completed,
                                total=len(markets),
                                skipped=total_already,
                                milestone_label="第100个市场里程碑",
                            )
                            progress["hundred_notified"] = True
                        if total_completed >= progress["last_milestone"] + 1000:
                            send_lark_notification_progress(
                                data_name=DATA_LABEL,
                                completed=total_completed,
                                total=len(markets),
                                skipped=total_already,
                                milestone_label="进度",
                            )
                            progress["last_milestone"] = (total_completed // 1000) * 1000
                        if progress["success"] % 100 == 0:
                            logger.info(
                                f"进度：本轮新增 {progress['success']}，累计 {total_completed}/{len(markets)}"
                            )
            except Exception as e:
                logger.exception(f"{slug} 处理异常: {e}")

    total_completed = total_already + progress["success"]

    readme = f"""# {DATA_LABEL}

## 数据名称
{DATA_LABEL}

## 说明
- 内容：Polymarket 上 **BTC 5分钟方向预测** 市场的完整订单簿历史快照（Fully Order Book）
- 时间范围：**2026-01-01 00:00:00 UTC ~ 2026-03-01 23:59:59 UTC**
- 导出时间：**{RUN_STARTED_AT.isoformat(timespec="seconds")}**
- 数据来源：Dome API (https://docs.domeapi.io/)
- 市场数量：{len(markets)}
- 成功写入订单簿的市场数：{total_completed}

## 目录结构
- 输出根目录：`{OUTPUT_ROOT_DIR}`
- 导出目录：`{OUTPUT_FOLDER_NAME}`
- 命名规则：`{DATA_TYPE_LABEL}-时间戳-{INTERVAL_LABEL}`
- `markets_manifest.json`：市场列表与时间范围、数据名称等元信息
- `market_<YYYYMMDD_HHMMSS_UTC>/`：每个市场一个文件夹，使用该 5 分钟市场对应的 UTC 时间戳命名
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
    logger.info("=" * 60)

    send_lark_notification(
        data_name=DATA_LABEL,
        time_start=str(START_DATE.date()),
        time_end=str(END_DATE.date()),
        total=len(markets),
        success=total_completed,
        output_path=str(OUTPUT_DIR),
        skipped=total_already,
        new_written=progress["success"],
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
