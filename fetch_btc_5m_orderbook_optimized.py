#!/usr/bin/env python3
"""
Dome API - BTC 5M Fully Order Book（优化版 - 8 API Key 并发 + gzip 压缩 + 数据完整性保障）

- 8 个 API Key 轮询，每 Key 独立限速
- 时间范围：2026-02-12 ～ 2026-03-01（UTC，固定周期）
- 输出根目录: Mac 桌面 / BTC-5M-FullyOrderBook-20260212-20260301
- gzip 压缩 orderbook 文件（体积减少 80-90%）
- 原子写入：临时目录 + rename，防止中断导致不完整
- 空数据二次确认：API 返回空时换 Key 重试确认
- 写入后立即校验 .gz 文件可读性
- 全量验证 + 自动补跑失败市场（最多 2 轮）
- 生成 verification_report.json
"""

import gzip
import json
import os
import shutil
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

OUTPUT_ROOT_DIR = Path("/Users/ocean/Desktop")
DATA_LABEL = "BTC Polymarket Fully Order Book 5M (2026-02-12 to 2026-03-01)"
DATA_TYPE_LABEL = "BTC-Poly-FullyOrderBook"
INTERVAL_LABEL = "5M"
MARKET_SLUG_PREFIX = "btc-updown-5m"
MARKET_DISPLAY_LABEL = "BTC 5M"
RUN_STARTED_AT = datetime.now().astimezone()
RUN_TIMESTAMP = os.getenv("DOME_OUTPUT_TIMESTAMP") or RUN_STARTED_AT.strftime("%Y%m%d_%H%M%S")
OUTPUT_FOLDER_NAME = "BTC-5M-FullyOrderBook-20260212-20260301"
OUTPUT_DIR = OUTPUT_ROOT_DIR / OUTPUT_FOLDER_NAME
LOG_FILE = OUTPUT_ROOT_DIR / "fetch_btc_orderbook_optimized_log.txt"
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
# 8 个 API Key 轮询使用
API_KEYS = [
    "aafc0f70ecaa32f3db1645a2024ab90c0d77526b",
    "0da7703a7b02171f28bc0565f6b92f1f84014a1b",
    "3488639103ead5fd2042436b63bca75dd0b4a441",
    "0ef6e5a21f8fd330199bc7b43b77d73b05ee6b74",
    "86670cfbb8f250345b71b0d099b0a6d4427f5112",
    "4ea34bc1d31292e351698e68e0605e1b980ed828",
    "7b297dd2df467443d346126a9c9b94e70df92cc7",
    "b7c1f5bbc85301b262412a041cf43c391733dddc",
]
CONCURRENCY = 40  # 8 Keys × 5 ≈ 40 workers
MARKET_FETCH_RETRIES = 3

LARK_WEBHOOK_URL = "https://open.larksuite.com/open-apis/bot/v2/hook/b7651f44-ff95-479f-a42e-78566de0a916"


# ---------------------------------------------------------------------------
# Rate Limiter
# ---------------------------------------------------------------------------
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


_rate_limiters = [TokenBucketRateLimiter(rate=7.0, burst=5) for _ in range(len(API_KEYS))]
_key_index = 0
_key_lock = threading.Lock()


def _next_key_and_limiter() -> Tuple[str, TokenBucketRateLimiter]:
    global _key_index
    with _key_lock:
        i = _key_index % len(API_KEYS)
        _key_index += 1
    return API_KEYS[i], _rate_limiters[i]


def _specific_key_and_limiter(key_offset: int) -> Tuple[str, TokenBucketRateLimiter]:
    """获取一个与上次不同的 Key（用于空数据二次确认）"""
    with _key_lock:
        i = key_offset % len(API_KEYS)
    return API_KEYS[i], _rate_limiters[i]


# ---------------------------------------------------------------------------
# Lark Notifications
# ---------------------------------------------------------------------------
def send_lark_notification_start(
    data_name: str, time_start: str, time_end: str, total: int, pending: int, output_path: str
):
    completed = max(total - pending, 0)
    text = (
        f"【Dome API 数据拉取已启动 - 优化版】\n\n"
        f"数据名称：{data_name}\n"
        f"时间范围：{time_start} ~ {time_end}\n"
        f"市场总数：{total}\n"
        f"已完成累计：{completed}\n"
        f"剩余待跑：{pending}\n"
        f"保存位置：{output_path}\n"
        f"优化：gzip 压缩 + 原子写入 + 空数据二次确认\n\n"
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
        f"【Dome API 数据拉取完成 - 优化版】\n\n"
        f"数据名称：{data_name}\n"
        f"时间范围：{time_start} ~ {time_end}\n"
        f"市场总数：{total}\n"
        f"已完成累计：{success}\n"
        f"本轮新增：{new_written}\n"
        f"已跳过历史：{skipped}\n"
        f"剩余待跑：{remaining}\n"
        f"保存位置：{output_path}\n\n"
        f"任务已全部跑完。（优化版：8 Key 并发 + gzip 压缩）"
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


# ---------------------------------------------------------------------------
# API Request
# ---------------------------------------------------------------------------
def api_request(url: str, params: dict, max_retries: int = 5) -> Optional[Dict]:
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


def api_request_with_specific_key(url: str, params: dict, key_offset: int, max_retries: int = 3) -> Optional[Dict]:
    """使用指定偏移的 Key 发起请求（用于空数据二次确认）"""
    for attempt in range(max_retries):
        api_key, limiter = _specific_key_and_limiter(key_offset + attempt)
        limiter.acquire()
        headers = {"X-API-Key": api_key, "Content-Type": "application/json"}
        try:
            resp = requests.get(url, headers=headers, params=params, timeout=30)
            if resp.status_code == 200:
                return resp.json()
            elif resp.status_code == 429:
                wait = 2 ** (attempt + 1)
                time.sleep(wait)
            elif resp.status_code in (502, 503):
                time.sleep(5 * (attempt + 1))
            else:
                return None
        except (requests.exceptions.Timeout, requests.exceptions.ConnectionError):
            time.sleep(3)
    return None


# ---------------------------------------------------------------------------
# Fetch Markets
# ---------------------------------------------------------------------------
def fetch_markets() -> List[Dict]:
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
            logger.info("manifest 时间范围与本次不一致，从 API 拉取市场列表")

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


# ---------------------------------------------------------------------------
# Market Directory Helpers
# ---------------------------------------------------------------------------
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
    has_metadata = (market_dir / "metadata.json").exists()
    has_side_a = (
        (market_dir / "orderbook_side_a.json").exists()
        or (market_dir / "orderbook_side_a.json.gz").exists()
    )
    has_side_b = (
        (market_dir / "orderbook_side_b.json").exists()
        or (market_dir / "orderbook_side_b.json.gz").exists()
    )
    return has_metadata and has_side_a and has_side_b


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


# ---------------------------------------------------------------------------
# Snapshot Helpers
# ---------------------------------------------------------------------------
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


def validate_snapshot(snapshot: Dict) -> bool:
    """校验单个 snapshot 的基本结构"""
    if not isinstance(snapshot, dict):
        return False
    if "asks" not in snapshot or "bids" not in snapshot:
        return False
    if not isinstance(snapshot["asks"], list) or not isinstance(snapshot["bids"], list):
        return False
    return True


# ---------------------------------------------------------------------------
# Orderbook Fetching with Empty-Data Confirmation
# ---------------------------------------------------------------------------
def fetch_orderbook_snapshots(token_id: str, start_ms: int, end_ms: int) -> List[Dict]:
    """拉取 orderbook 快照，带分页"""
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
        snapshots = data.get("snapshots", [])
        # 校验每个 snapshot 的结构
        for s in snapshots:
            if not validate_snapshot(s):
                logger.warning(f"token_id={token_id} 收到结构异常的 snapshot，跳过: {str(s)[:100]}")
                continue
            all_snapshots.append(s)
        pagination = data.get("pagination", {})
        if not pagination.get("has_more") or not pagination.get("pagination_key"):
            break
        pagination_key = pagination.get("pagination_key")
    return normalize_snapshots(all_snapshots)


def fetch_orderbook_with_empty_confirmation(
    token_id: str, start_ms: int, end_ms: int
) -> Tuple[List[Dict], bool]:
    """
    拉取 orderbook 快照。如果首次返回空，换 Key 再试一次确认。
    返回 (snapshots, empty_confirmed)
      - empty_confirmed=True 表示确认确实无数据
    """
    snapshots = fetch_orderbook_snapshots(token_id, start_ms, end_ms)
    if snapshots:
        return snapshots, False

    # 首次返回空 → 换一个不同的 Key 再确认一次
    logger.info(f"token_id={token_id[:30]}... 首次返回空，换 Key 二次确认")
    url = f"{API_BASE_URL}/polymarket/orderbooks"
    params = {
        "token_id": token_id,
        "start_time": start_ms,
        "end_time": end_ms,
        "limit": 200,
    }
    # 用一个随机偏移的 Key
    import random
    key_offset = random.randint(0, len(API_KEYS) - 1)
    data = api_request_with_specific_key(url, params, key_offset)
    if data:
        retry_snapshots = data.get("snapshots", [])
        valid = [s for s in retry_snapshots if validate_snapshot(s)]
        if valid:
            logger.info(f"token_id={token_id[:30]}... 二次确认返回 {len(valid)} 条快照（首次为空是偶发问题）")
            return normalize_snapshots(valid), False

    logger.info(f"token_id={token_id[:30]}... 二次确认仍为空，标记为确实无数据")
    return [], True


# ---------------------------------------------------------------------------
# Gzip JSON Writer + Verification
# ---------------------------------------------------------------------------
def write_json_gz(filepath: Path, data, *, ensure_ascii: bool = True) -> Path:
    """写入 gzip 压缩的 JSON 文件，返回实际写入的文件路径"""
    gz_path = filepath.with_suffix(filepath.suffix + ".gz")
    json_bytes = json.dumps(data, ensure_ascii=ensure_ascii, indent=2).encode("utf-8")
    with gzip.open(gz_path, "wb", compresslevel=6) as f:
        f.write(json_bytes)
    return gz_path


def verify_gz_file(gz_path: Path, expected_count: Optional[int] = None) -> bool:
    """校验 .gz 文件可读且内容一致"""
    try:
        with gzip.open(gz_path, "rt", encoding="utf-8") as f:
            data = json.load(f)
        if expected_count is not None:
            if isinstance(data, list) and len(data) != expected_count:
                logger.error(f"校验失败 {gz_path.name}: 预期 {expected_count} 条，实际 {len(data)} 条")
                return False
        return True
    except Exception as e:
        logger.error(f"校验失败 {gz_path.name}: {e}")
        return False


# ---------------------------------------------------------------------------
# Full Verification Pass
# ---------------------------------------------------------------------------
def verify_all_market_dirs(markets: List[Dict], existing_market_dirs: Dict[str, Path]) -> List[Dict]:
    """
    遍历所有市场目录，验证完整性。返回需要重跑的市场列表。
    """
    failed_markets = []
    verified = 0
    empty_data = 0
    for market in markets:
        cid = market.get("condition_id", "")
        market_dir = existing_market_dirs.get(cid)
        if not market_dir or not market_dir.exists():
            failed_markets.append(market)
            continue

        # 检查 metadata
        metadata_path = market_dir / "metadata.json"
        if not metadata_path.exists():
            logger.warning(f"验证失败 {market_dir.name}: 缺少 metadata.json")
            failed_markets.append(market)
            continue

        try:
            with open(metadata_path, "r", encoding="utf-8") as f:
                meta = json.load(f)
        except Exception as e:
            logger.warning(f"验证失败 {market_dir.name}: metadata.json 不可读: {e}")
            failed_markets.append(market)
            continue

        # 检查 orderbook 文件
        for side_name in ("orderbook_side_a", "orderbook_side_b"):
            gz_path = market_dir / f"{side_name}.json.gz"
            plain_path = market_dir / f"{side_name}.json"
            if gz_path.exists():
                if not verify_gz_file(gz_path):
                    logger.warning(f"验证失败 {market_dir.name}: {gz_path.name} 损坏")
                    failed_markets.append(market)
                    break
            elif plain_path.exists():
                try:
                    with open(plain_path, "r", encoding="utf-8") as f:
                        json.load(f)
                except Exception:
                    logger.warning(f"验证失败 {market_dir.name}: {plain_path.name} 损坏")
                    failed_markets.append(market)
                    break
            else:
                logger.warning(f"验证失败 {market_dir.name}: 缺少 {side_name}")
                failed_markets.append(market)
                break
        else:
            # 两边都通过
            if meta.get("_orderbook_data_available") is False:
                empty_data += 1
            verified += 1

    logger.info(f"全量验证完成：{verified} 个通过（其中 {empty_data} 个无数据），{len(failed_markets)} 个失败需重跑")
    return failed_markets


def generate_verification_report(
    markets: List[Dict], existing_market_dirs: Dict[str, Path], output_dir: Path
):
    """生成 verification_report.json"""
    report = {
        "generated_at": datetime.now().astimezone().isoformat(timespec="seconds"),
        "total_markets": len(markets),
        "summary": {"complete": 0, "empty_data": 0, "failed": 0},
        "markets": [],
    }
    for market in markets:
        cid = market.get("condition_id", "")
        slug = market.get("market_slug", "")
        market_dir = existing_market_dirs.get(cid)
        entry = {
            "market_slug": slug,
            "condition_id": cid,
            "dir_name": market_dir.name if market_dir else None,
        }
        if market_dir and is_market_dir_complete(market_dir):
            metadata_path = market_dir / "metadata.json"
            try:
                with open(metadata_path, "r", encoding="utf-8") as f:
                    meta = json.load(f)
                if meta.get("_orderbook_data_available") is False:
                    entry["status"] = "empty_data"
                    entry["empty_confirmed"] = meta.get("_empty_confirmed_by_retry", False)
                    report["summary"]["empty_data"] += 1
                else:
                    entry["status"] = "complete"
                    entry["side_a_count"] = meta.get("_side_a_snapshot_count", "?")
                    entry["side_b_count"] = meta.get("_side_b_snapshot_count", "?")
                    report["summary"]["complete"] += 1
            except Exception:
                entry["status"] = "failed"
                report["summary"]["failed"] += 1
        else:
            entry["status"] = "failed"
            report["summary"]["failed"] += 1
        report["markets"].append(entry)

    report_path = output_dir / "verification_report.json"
    with open(report_path, "w", encoding="utf-8") as f:
        json.dump(report, f, ensure_ascii=False, indent=2)
    logger.info(f"验证报告已生成: {report_path}")
    logger.info(
        f"  完整: {report['summary']['complete']}, "
        f"空数据: {report['summary']['empty_data']}, "
        f"失败: {report['summary']['failed']}"
    )
    return report


# ---------------------------------------------------------------------------
# Write Lock
# ---------------------------------------------------------------------------
_write_lock = threading.Lock()


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    logger.info("=" * 60)
    logger.info("Dome API - BTC 5M Fully Order Book（优化版 - 8 Key 并发 + gzip）")
    logger.info("=" * 60)
    logger.info(f"时间范围(UTC): {START_DATE.isoformat()} ~ {END_DATE.isoformat()}")
    logger.info(f"输出目录: {OUTPUT_DIR}")
    logger.info(f"API Keys: {len(API_KEYS)} 个, Workers: {CONCURRENCY}")

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    # 清理残留的临时目录
    for p in OUTPUT_DIR.iterdir():
        if p.is_dir() and p.name.startswith(".tmp_"):
            logger.warning(f"清理残留临时目录: {p.name}")
            shutil.rmtree(p, ignore_errors=True)

    markets = fetch_markets()
    logger.info(f"共 {len(markets)} 个 {MARKET_DISPLAY_LABEL} 市场")

    if not markets:
        logger.warning("未获取到任何市场")
        send_lark_notification(
            data_name=DATA_LABEL,
            time_start=str(START_DATE.date()),
            time_end=str(END_DATE.date()),
            total=0, success=0,
            output_path=str(OUTPUT_DIR),
            skipped=0, new_written=0,
        )
        return 0, 0

    # 写入 manifest
    manifest_path = OUTPUT_DIR / "markets_manifest.json"
    manifest = {
        "data_name": DATA_LABEL,
        "description": "Polymarket BTC 5M direction markets fully order book snapshots, 2026-02-12 to 2026-03-01 UTC",
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
            "compression": "gzip",
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

        # 拉取 orderbook，带重试和空数据二次确认
        last_error = None
        ob_a, ob_b = [], []
        empty_confirmed_a, empty_confirmed_b = False, False
        for attempt in range(MARKET_FETCH_RETRIES):
            try:
                with ThreadPoolExecutor(max_workers=2) as side_pool:
                    fa = side_pool.submit(fetch_orderbook_with_empty_confirmation, token_a, q_start, q_end)
                    fb = side_pool.submit(fetch_orderbook_with_empty_confirmation, token_b, q_start, q_end)
                    ob_a, empty_confirmed_a = fa.result()
                    ob_b, empty_confirmed_b = fb.result()
                break
            except Exception as e:
                last_error = e
                wait = 3 * (attempt + 1)
                logger.warning(f"{slug} 拉取失败，第 {attempt + 1} 次重试，等待 {wait}s：{e}")
                time.sleep(wait)
        else:
            raise RuntimeError(f"{slug} 多次重试后仍失败：{last_error}")

        has_orderbook_data = bool(ob_a or ob_b)

        # 准备增强后的 metadata
        market_meta = dict(market)
        market_meta["_orderbook_data_available"] = has_orderbook_data
        market_meta["_side_a_snapshot_count"] = len(ob_a)
        market_meta["_side_b_snapshot_count"] = len(ob_b)
        if not has_orderbook_data:
            market_meta["_empty_confirmed_by_retry"] = empty_confirmed_a and empty_confirmed_b

        # 原子写入：临时目录 → rename
        with _write_lock:
            existing_market_dir = existing_market_dirs.get(cid)
            should_force_refresh = cid in force_refresh_cids
            if existing_market_dir and is_market_dir_complete(existing_market_dir) and not should_force_refresh:
                logger.debug(f"{slug} 已存在，跳过写入（防重复）")
                return True
            market_dir = resolve_market_dir(market, cid, existing_market_dirs)

            tmp_dir = market_dir.parent / f".tmp_{market_dir.name}_{threading.get_ident()}"
            try:
                tmp_dir.mkdir(parents=True, exist_ok=True)

                # 1. 先写 orderbook 文件（gzip）
                gz_a = write_json_gz(tmp_dir / "orderbook_side_a.json", ob_a)
                gz_b = write_json_gz(tmp_dir / "orderbook_side_b.json", ob_b)

                # 2. 写入后立即校验
                if not verify_gz_file(gz_a, expected_count=len(ob_a)):
                    raise RuntimeError(f"{slug} orderbook_side_a.json.gz 写入校验失败")
                if not verify_gz_file(gz_b, expected_count=len(ob_b)):
                    raise RuntimeError(f"{slug} orderbook_side_b.json.gz 写入校验失败")

                # 3. 写 marker 文件（如果无数据）
                if not has_orderbook_data:
                    marker_path = tmp_dir / "NO_ORDERBOOK_DATA.marker"
                    marker_path.write_text(
                        f"This market has no orderbook data for the queried time range.\n"
                        f"condition_id: {cid}\n"
                        f"market_slug: {slug}\n"
                        f"query_range_ms: {q_start} - {q_end}\n"
                        f"empty_confirmed_by_retry: {empty_confirmed_a and empty_confirmed_b}\n",
                        encoding="utf-8",
                    )

                # 4. 最后写 metadata（作为完成标记）
                with open(tmp_dir / "metadata.json", "w", encoding="utf-8") as f:
                    json.dump(market_meta, f, ensure_ascii=False, indent=2)

                # 5. 原子 rename
                if market_dir.exists():
                    shutil.rmtree(market_dir)
                tmp_dir.rename(market_dir)

            except Exception:
                if tmp_dir.exists():
                    shutil.rmtree(tmp_dir, ignore_errors=True)
                raise

            existing_market_dirs[cid] = market_dir
            if should_force_refresh:
                force_refresh_cids.discard(cid)
                persist_force_refresh_cids(force_refresh_cids)

        if has_orderbook_data:
            logger.info(f"{slug}  Side A: {len(ob_a)}, Side B: {len(ob_b)} 条快照 (gzip)")
        else:
            logger.info(f"{slug}  NO ORDERBOOK DATA (已标记)")
        return True

    # --- 主拉取循环 ---
    def run_fetch_batch(batch: List[Dict], batch_label: str = ""):
        nonlocal total_already
        batch_success = 0
        with ThreadPoolExecutor(max_workers=CONCURRENCY) as pool:
            futures = {pool.submit(process_one_market, m): m for m in batch}
            for future in as_completed(futures):
                market = futures[future]
                slug = market.get("market_slug", "")
                try:
                    ok = future.result()
                    if ok:
                        batch_success += 1
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
                                    f"进度{batch_label}：本轮新增 {progress['success']}，累计 {total_completed}/{len(markets)}"
                                )
                except Exception as e:
                    logger.exception(f"{slug} 处理异常: {e}")
        return batch_success

    # 主拉取
    run_fetch_batch(pending)

    # --- 全量验证 + 自动补跑（最多 2 轮）---
    for retry_round in range(1, 3):
        # 重新扫描以获取最新状态
        existing_market_dirs = get_existing_completed_market_dirs()
        failed_markets = verify_all_market_dirs(markets, existing_market_dirs)
        if not failed_markets:
            logger.info("全量验证通过，无需补跑")
            break
        logger.info(f"第 {retry_round} 轮补跑：{len(failed_markets)} 个失败市场")
        # 清理失败市场的不完整目录
        for fm in failed_markets:
            fm_cid = fm.get("condition_id", "")
            fm_dir = existing_market_dirs.get(fm_cid)
            if fm_dir and fm_dir.exists() and not is_market_dir_complete(fm_dir):
                shutil.rmtree(fm_dir, ignore_errors=True)
                existing_market_dirs.pop(fm_cid, None)
        run_fetch_batch(failed_markets, batch_label=f"（补跑第{retry_round}轮）")
    else:
        # 最终再扫描一次
        existing_market_dirs = get_existing_completed_market_dirs()

    total_completed = len([
        cid for cid in (m.get("condition_id", "") for m in markets)
        if cid in existing_market_dirs
    ])

    # 生成验证报告
    generate_verification_report(markets, existing_market_dirs, OUTPUT_DIR)

    readme = f"""# {DATA_LABEL}

## 数据名称
{DATA_LABEL}

## 说明
- 内容：Polymarket 上 **BTC 5分钟方向预测** 市场的完整订单簿历史快照（Fully Order Book）
- 时间范围：**2026-02-12 00:00:00 UTC ~ 2026-03-01 23:59:59 UTC**
- 导出时间：**{RUN_STARTED_AT.isoformat(timespec="seconds")}**
- 数据来源：Dome API (https://docs.domeapi.io/)
- 市场数量：{len(markets)}
- 成功写入订单簿的市场数：{total_completed}

## 优化
- Orderbook 文件使用 **gzip 压缩**（.json.gz），体积减少约 80-90%
- 使用 `gzip.open(path, 'rt')` 或 `gunzip` 命令解压即可读取
- JSON 内容保持 indent=2 格式，解压后与原始数据一致

## 数据完整性保障
- 空数据二次确认：API 返回空时自动换 Key 重试确认
- 写入后立即校验：.gz 文件写入后解压验证
- 全量验证 + 自动补跑失败市场（最多 2 轮）
- `verification_report.json`：每个市场的最终状态

## 目录结构
- `markets_manifest.json`：市场列表与时间范围等元信息
- `verification_report.json`：所有市场的验证报告
- `market_<YYYYMMDD_HHMMSS_UTC>/`：每个市场一个文件夹
  - `metadata.json`：市场基本信息 + 数据可用性标记
  - `orderbook_side_a.json.gz`：Up 方向订单簿快照数组（gzip）
  - `orderbook_side_b.json.gz`：Down 方向订单簿快照数组（gzip）
  - `NO_ORDERBOOK_DATA.marker`（仅无数据时）：标记该市场无 orderbook 数据

## 读取 gzip 文件示例（Python）
```python
import gzip, json
with gzip.open("orderbook_side_a.json.gz", "rt", encoding="utf-8") as f:
    snapshots = json.load(f)
```

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
