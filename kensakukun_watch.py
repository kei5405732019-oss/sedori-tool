#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
kensakukun_watch.py
--------------------
買取けんさく君（https://www.kaitorikensakukun.com/）のトップページに表示される
「リアルタイム買取情報」（最新11件程度の依頼ログ：日時・都道府県・品名）を
定期的に取得し、SQLiteに蓄積する。

新着エントリはcatalog.dbと突き合わせてカテゴリー・メーカー・買取価格を付与する。
catalog.dbはcatalog_crawler.pyが別途（日次cron）で生成・更新する。

実行例:
  python3 kensakukun_watch.py            # 1回ポーリングして終了
  python3 kensakukun_watch.py --loop 300 # 300秒間隔で無限ループ

cron想定（5分おき）:
  */5 * * * * /usr/bin/python3 /home/kei5/kensakukun_watch/kensakukun_watch.py >> /home/kei5/kensakukun_watch/run.log 2>&1
"""

import argparse
import datetime
import json
import logging
import re
import sqlite3
import sys
import time
from pathlib import Path

import requests
from bs4 import BeautifulSoup

# ============== 設定 ==============
BASE_DIR = Path(__file__).resolve().parent
DB_PATH = BASE_DIR / "kensakukun_history.db"
CATALOG_DB_PATH = BASE_DIR / "catalog.db"
STATE_PATH = BASE_DIR / "last_snapshot.json"
LOG_PATH = BASE_DIR / "watch.log"
DASHBOARD_JSON_PATH = BASE_DIR / "dashboard_data.json"
DASHBOARD_EXPORT_LIMIT = 2000

URL = "https://www.kaitorikensakukun.com/"
DISCORD_WEBHOOK_URL = ""
GAS_URL = "https://script.google.com/macros/s/AKfycbzhJe9pf9W6950yYyusUHJ-uo-bbNLUc87MKRssEAjxfQqU4IZdi0l_bolgAOyvswuxKQ/exec"
GAS_ACCESS_KEY = "katuragiya"
GAS_TIMEOUT = 15

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/124.0 Safari/537.36"
    ),
    "Accept-Language": "ja,en;q=0.8",
}

REQUEST_TIMEOUT = 15
REQUEST_INTERVAL_MIN_SEC = 240
DATE_RE = re.compile(r"(\d{4})/(\d{2})/(\d{2})（(\d{1,2})時(\d{1,2})分）")
ITEM_RE = re.compile(r"【(.+?)】のお客様から(.+?)のご依頼いただきました。")

# ============== ログ設定 ==============
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(LOG_PATH, encoding="utf-8"),
        logging.StreamHandler(sys.stdout),
    ],
)
logger = logging.getLogger(__name__)


# ============== DB ==============
def init_db() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            requested_at TEXT NOT NULL,
            prefecture TEXT NOT NULL,
            item_name TEXT NOT NULL,
            top_category TEXT,
            sub_category TEXT,
            maker TEXT,
            price INTEGER,
            fetched_at TEXT NOT NULL
        )
        """
    )
    # 既存DBへのカラム追加（既に存在する場合はエラーを無視）
    for col, coltype in [
        ("top_category", "TEXT"),
        ("sub_category", "TEXT"),
        ("maker", "TEXT"),
        ("price", "INTEGER"),
    ]:
        try:
            conn.execute(f"ALTER TABLE history ADD COLUMN {col} {coltype}")
        except sqlite3.OperationalError:
            pass  # 既に存在する場合は無視
    conn.execute("CREATE INDEX IF NOT EXISTS idx_item_name ON history(item_name)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_requested_at ON history(requested_at)")
    conn.commit()
    return conn


def insert_entries(conn: sqlite3.Connection, entries):
    fetched_at = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    conn.executemany(
        "INSERT INTO history (requested_at, prefecture, item_name, top_category, sub_category, maker, price, fetched_at) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        [
            (
                e["requested_at"],
                e["prefecture"],
                e["item_name"],
                e.get("top_category"),
                e.get("sub_category"),
                e.get("maker"),
                e.get("price"),
                fetched_at,
            )
            for e in entries
        ],
    )
    conn.commit()


# ============== カタログ突き合わせ ==============
def lookup_catalog(item_name: str) -> dict:
    """
    catalog.dbからitem_nameに一致するカテゴリー・メーカー・価格を返す。
    catalog.dbが存在しない、またはヒットしない場合はNoneのdictを返す。
    新品を優先し、なければ中古も使う。
    """
    empty = {"top_category": None, "sub_category": None, "maker": None, "price": None}
    if not CATALOG_DB_PATH.exists():
        return empty
    try:
        cat_conn = sqlite3.connect(CATALOG_DB_PATH)
        cur = cat_conn.execute(
            "SELECT top_category, sub_category, maker, price FROM catalog "
            "WHERE item_name = ? ORDER BY CASE condition WHEN '新品' THEN 0 ELSE 1 END LIMIT 1",
            (item_name,),
        )
        row = cur.fetchone()
        cat_conn.close()
        if row:
            return {
                "top_category": row[0],
                "sub_category": row[1],
                "maker": row[2],
                "price": row[3],
            }
    except Exception as exc:
        logger.warning("カタログ参照失敗: %s", exc)
    return empty


# ============== 取得・解析 ==============
def fetch_html() -> str:
    resp = requests.get(URL, headers=HEADERS, timeout=REQUEST_TIMEOUT)
    resp.raise_for_status()
    resp.encoding = resp.apparent_encoding or "utf-8"
    return resp.text


def parse_entries(html: str):
    soup = BeautifulSoup(html, "html.parser")
    entries = []
    for dt in soup.find_all("dt"):
        date_text = dt.get_text(strip=True)
        m_date = DATE_RE.search(date_text)
        if not m_date:
            continue
        dd = dt.find_next_sibling("dd")
        if dd is None:
            continue
        item_text = dd.get_text(strip=True)
        m_item = ITEM_RE.search(item_text)
        if not m_item:
            continue
        y, mo, d, hh, mi = m_date.groups()
        requested_at = f"{y}-{mo}-{d} {int(hh):02d}:{int(mi):02d}"
        prefecture, item_name = m_item.groups()
        entries.append({"requested_at": requested_at, "prefecture": prefecture, "item_name": item_name})
    return entries


def fingerprint(e: dict) -> str:
    return f"{e['requested_at']}|{e['prefecture']}|{e['item_name']}"


# ============== 差分判定 ==============
def load_prev_snapshot():
    if not STATE_PATH.exists():
        return []
    try:
        return json.loads(STATE_PATH.read_text(encoding="utf-8"))
    except Exception:
        logger.warning("前回スナップショットの読み込みに失敗。空として扱います。")
        return []


def save_snapshot(entries):
    fps = [fingerprint(e) for e in entries]
    STATE_PATH.write_text(json.dumps(fps, ensure_ascii=False), encoding="utf-8")


def diff_new_entries(current_entries, prev_fps):
    if not prev_fps:
        return current_entries, False
    current_fps = [fingerprint(e) for e in current_entries]
    target = prev_fps[0]
    if target in current_fps:
        idx = current_fps.index(target)
        return current_entries[:idx], False
    return current_entries, True


# ============== ダッシュボード用エクスポート ==============
def export_dashboard_json(conn: sqlite3.Connection):
    cur = conn.execute(
        "SELECT requested_at, prefecture, item_name, top_category, sub_category, maker, price "
        "FROM history ORDER BY id DESC LIMIT ?",
        (DASHBOARD_EXPORT_LIMIT,),
    )
    rows = cur.fetchall()
    total_count = conn.execute("SELECT COUNT(*) FROM history").fetchone()[0]
    payload = {
        "generated_at": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "total_count": total_count,
        "exported_count": len(rows),
        "entries": [
            {
                "requested_at": r[0],
                "prefecture": r[1],
                "item_name": r[2],
                "top_category": r[3],
                "sub_category": r[4],
                "maker": r[5],
                "price": r[6],
            }
            for r in rows
        ],
    }
    tmp_path = DASHBOARD_JSON_PATH.with_suffix(".json.tmp")
    tmp_path.write_text(json.dumps(payload, ensure_ascii=False, indent=None), encoding="utf-8")
    tmp_path.replace(DASHBOARD_JSON_PATH)


# ============== GAS連携 ==============
def push_to_gas(new_entries):
    if not GAS_URL or not new_entries:
        return
    payload = {
        "key": GAS_ACCESS_KEY,
        "entries": [
            {
                "requested_at": e["requested_at"],
                "prefecture": e["prefecture"],
                "item_name": e["item_name"],
                "top_category": e.get("top_category"),
                "sub_category": e.get("sub_category"),
                "maker": e.get("maker"),
                "price": e.get("price"),
            }
            for e in new_entries
        ],
    }
    try:
        resp = requests.post(GAS_URL, json=payload, timeout=GAS_TIMEOUT)
        resp.raise_for_status()
        data = resp.json()
        if data.get("error"):
            logger.warning("GAS送信エラー: %s", data["error"])
        else:
            logger.info("GASへ %d 件送信しました。", data.get("inserted", len(new_entries)))
    except requests.RequestException as exc:
        logger.warning("GAS送信失敗（ネットワーク）: %s", exc)
    except ValueError:
        logger.warning("GASからの応答がJSONとして解釈できませんでした。")


# ============== Discord通知 ==============
def notify_discord(entries):
    if not DISCORD_WEBHOOK_URL or not entries:
        return
    lines = [f"・{e['requested_at']}｜{e['prefecture']}｜{e['item_name']}" for e in entries]
    content = "【けんさく君 新着買取依頼】\n" + "\n".join(lines)
    chunks = [content[i: i + 1900] for i in range(0, len(content), 1900)]
    for chunk in chunks:
        try:
            requests.post(DISCORD_WEBHOOK_URL, json={"content": chunk}, timeout=10)
        except Exception as exc:
            logger.warning("Discord通知に失敗: %s", exc)


# ============== メイン処理 ==============
def run_once():
    try:
        html = fetch_html()
    except requests.RequestException as exc:
        logger.error("ページ取得に失敗: %s", exc)
        return

    current_entries = parse_entries(html)
    if not current_entries:
        logger.warning("リアルタイム買取情報を抽出できませんでした。サイト構造が変わった可能性があります。")
        return

    prev_fps = load_prev_snapshot()
    new_entries, gap_warning = diff_new_entries(current_entries, prev_fps)

    if gap_warning:
        logger.warning("前回スナップショットがリストから消失。取りこぼしの可能性あり。")

    conn = init_db()

    if new_entries:
        # カタログ突き合わせ
        enriched = []
        for e in new_entries:
            cat = lookup_catalog(e["item_name"])
            enriched.append({**e, **cat})

        insert_entries(conn, list(reversed(enriched)))
        logger.info("新着 %d 件を記録しました。", len(enriched))
        for e in enriched:
            cat_label = e.get("top_category") or "カテゴリー不明"
            logger.info("  %s | %s | %s | %s", e["requested_at"], e["prefecture"], e["item_name"], cat_label)
        notify_discord(enriched)
        push_to_gas(enriched)
    else:
        logger.info("新着なし。")

    export_dashboard_json(conn)
    conn.close()
    save_snapshot(current_entries)


def main():
    parser = argparse.ArgumentParser(description="買取けんさく君 リアルタイム買取情報ウォッチャー")
    parser.add_argument("--loop", type=int, default=0)
    args = parser.parse_args()

    init_db()

    if args.loop <= 0:
        run_once()
        return

    interval = max(args.loop, REQUEST_INTERVAL_MIN_SEC)
    if interval != args.loop:
        logger.warning("間隔が短すぎるため %d 秒に調整しました。", interval)

    logger.info("ループ開始（%d 秒間隔）。Ctrl+Cで停止。", interval)
    try:
        while True:
            run_once()
            time.sleep(interval)
    except KeyboardInterrupt:
        logger.info("停止しました。")


if __name__ == "__main__":
    main()
