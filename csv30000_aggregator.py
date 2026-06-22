#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
csv30000_aggregator.py
----------------------
/home/kei5/CSV30000/ 配下の all_data_YYYYMMDDHHMM.csv を読み込み、
カテゴリー別・スナップショット別の集計をSQLiteに蓄積し、
ダッシュボード用の trend_data.json を出力する。

初回は全80ファイルを処理（数分かかる）。
2回目以降は未処理ファイルのみ差分処理するため高速。

cron（新CSVが生成された直後に実行）:
  0 23 * * * /usr/bin/python3 /home/kei5/kensakukun_watch/csv30000_aggregator.py >> /home/kei5/kensakukun_watch/aggregator.log 2>&1
"""

import csv
import json
import logging
import re
import sqlite3
import sys
from collections import defaultdict
from pathlib import Path

# ==================== 設定 ====================
BASE_DIR      = Path(__file__).resolve().parent
CSV_DIR       = Path("/home/kei5/CSV30000")
DB_PATH       = BASE_DIR / "price_trend.db"
JSON_PATH     = BASE_DIR / "trend_data.json"
LOG_PATH      = BASE_DIR / "aggregator.log"

# 出力するJSONに含めるカテゴリー上位件数
TOP_CATEGORIES = 12
# スプレッド分析で表示するカテゴリー数
TOP_SPREAD_CATS = 8

# (store_name, name_col, price_col, dt_col, category_col)  category_col=None は取得不可
STORES = [
    ("アバウテック", 10, 11, 12, 13),
    ("けんさく",     14, 15, 16, 17),
    ("商店",         18, 19, 20, 21),
    ("森森",         22, 23, 24, 25),
    ("一丁目",       26, 27, 28, 30),   # col29=リンク
    ("ウィキ",       31, 32, 33, 34),
    ("家電市場",     35, 36, 37, 38),
    ("モバイル一番", 39, 40, 41, 42),
    ("ルデヤ",       43, 44, 45, 46),
    ("ホムラ",       47, 48, 49, None),
    ("モバミ",       50, 51, 52, None),
    ("パンダ",       53, 54, 55, 56),
    ("楽園",         57, 58, 59, 60),
    ("エノキング",   61, 62, 63, 64),
]
STORE_NAMES = [s[0] for s in STORES]

FILENAME_RE = re.compile(r'all_data_(\d{4})(\d{2})(\d{2})(\d{2})(\d{2})\.csv$')

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(LOG_PATH, encoding="utf-8"),
        logging.StreamHandler(sys.stdout),
    ],
)
logger = logging.getLogger(__name__)


# ==================== DB初期化 ====================
def init_db() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS processed_files (
            filename TEXT PRIMARY KEY,
            processed_at TEXT NOT NULL
        )
    """)
    # カテゴリー別集計（スナップショット日時 × カテゴリー）
    conn.execute("""
        CREATE TABLE IF NOT EXISTS category_snapshot (
            snapshot_dt  TEXT NOT NULL,
            category     TEXT NOT NULL,
            item_count   INTEGER NOT NULL DEFAULT 0,
            priced_count INTEGER NOT NULL DEFAULT 0,
            avg_buyback  REAL,
            avg_amazon   REAL,
            avg_spread   REAL,
            spread_positive INTEGER NOT NULL DEFAULT 0,
            PRIMARY KEY (snapshot_dt, category)
        )
    """)
    # 店舗別集計（スナップショット × カテゴリー × 店舗）
    conn.execute("""
        CREATE TABLE IF NOT EXISTS store_snapshot (
            snapshot_dt  TEXT NOT NULL,
            category     TEXT NOT NULL,
            store        TEXT NOT NULL,
            priced_count INTEGER NOT NULL DEFAULT 0,
            avg_price    REAL,
            PRIMARY KEY (snapshot_dt, category, store)
        )
    """)
    conn.commit()
    return conn


# ==================== CSV処理 ====================
def safe_int(val: str) -> int:
    try:
        v = int(str(val).strip().replace(',', ''))
        return v if v > 0 else 0
    except (ValueError, TypeError):
        return 0


def parse_filename_dt(fname: str) -> str | None:
    m = FILENAME_RE.search(fname)
    if not m:
        return None
    y, mo, d, h, mi = m.groups()
    return f"{y}-{mo}-{d} {h}:{mi}"


def process_csv(path: Path, snapshot_dt: str, conn: sqlite3.Connection):
    """1ファイルを読み込み、集計をDBに書き込む"""
    # カテゴリー別集計バッファ
    # cat → {item_count, priced_count, buyback_sum, amazon_sum, spread_sum, spread_positive}
    cat_buf = defaultdict(lambda: {
        "item_count": 0, "priced_count": 0,
        "buyback_sum": 0, "amazon_sum": 0, "amazon_count": 0,
        "spread_sum": 0, "spread_positive": 0,
    })
    # 店舗別集計バッファ
    # (cat, store) → {priced_count, price_sum}
    store_buf = defaultdict(lambda: {"priced_count": 0, "price_sum": 0})

    MAX_COLS = max(max(s[1], s[2], s[3], s[4] if s[4] else 0) for s in STORES) + 2

    try:
        with open(path, encoding="shift_jis", errors="replace", newline="") as f:
            reader = csv.reader(f)
            next(reader)  # ヘッダースキップ
            for row in reader:
                if len(row) < 10:
                    continue

                # Amazon価格（カート価格を優先、なければ最低価格）
                amazon_cart = safe_int(row[2] if len(row) > 2 else "")
                amazon_min  = safe_int(row[6] if len(row) > 6 else "")
                amazon_price = amazon_cart if amazon_cart > 0 else amazon_min

                # 各店舗の買取価格とカテゴリーを取得
                store_prices = {}
                categories   = []
                for store_name, name_col, price_col, dt_col, cat_col in STORES:
                    if price_col >= len(row):
                        continue
                    price = safe_int(row[price_col])
                    store_prices[store_name] = price
                    if cat_col and cat_col < len(row):
                        cat_val = row[cat_col].strip()
                        if cat_val:
                            categories.append(cat_val)

                # カテゴリーを決定（最も多く出てくる値、なければ「不明」）
                category = "不明"
                if categories:
                    freq = defaultdict(int)
                    for c in categories:
                        freq[c] += 1
                    category = max(freq, key=freq.get)

                # 最良買取価格
                best_buyback = max(store_prices.values()) if store_prices else 0

                # カテゴリー別集計
                b = cat_buf[category]
                b["item_count"] += 1
                if best_buyback > 0:
                    b["priced_count"] += 1
                    b["buyback_sum"] += best_buyback
                    if amazon_price > 0:
                        spread = amazon_price - best_buyback
                        b["amazon_sum"] += amazon_price
                        b["amazon_count"] += 1
                        b["spread_sum"] += spread
                        if spread > 0:
                            b["spread_positive"] += 1

                # 店舗別集計
                for store_name, price in store_prices.items():
                    if price > 0:
                        sb = store_buf[(category, store_name)]
                        sb["priced_count"] += 1
                        sb["price_sum"] += price

    except Exception as exc:
        logger.error("CSV読み込みエラー %s: %s", path.name, exc)
        return

    # DB書き込み
    for cat, b in cat_buf.items():
        avg_buyback = (b["buyback_sum"] / b["priced_count"]) if b["priced_count"] > 0 else None
        avg_amazon  = (b["amazon_sum"] / b["amazon_count"]) if b["amazon_count"] > 0 else None
        avg_spread  = (b["spread_sum"] / b["amazon_count"]) if b["amazon_count"] > 0 else None
        conn.execute("""
            INSERT OR REPLACE INTO category_snapshot
            (snapshot_dt, category, item_count, priced_count, avg_buyback, avg_amazon, avg_spread, spread_positive)
            VALUES (?,?,?,?,?,?,?,?)
        """, (snapshot_dt, cat, b["item_count"], b["priced_count"],
              avg_buyback, avg_amazon, avg_spread, b["spread_positive"]))

    for (cat, store), sb in store_buf.items():
        avg_price = sb["price_sum"] / sb["priced_count"] if sb["priced_count"] > 0 else None
        conn.execute("""
            INSERT OR REPLACE INTO store_snapshot
            (snapshot_dt, category, store, priced_count, avg_price)
            VALUES (?,?,?,?,?)
        """, (snapshot_dt, cat, store, sb["priced_count"], avg_price))

    from datetime import datetime as _dt
    conn.execute(
        "INSERT OR REPLACE INTO processed_files (filename, processed_at) VALUES (?,?)",
        (path.name, _dt.now().strftime("%Y-%m-%d %H:%M:%S"))
    )
    conn.commit()


# ==================== JSON出力 ====================
def export_json(conn: sqlite3.Connection):
    # 全スナップショット日時（昇順）
    rows = conn.execute(
        "SELECT DISTINCT snapshot_dt FROM category_snapshot ORDER BY snapshot_dt"
    ).fetchall()
    snapshots = [r[0] for r in rows]
    if not snapshots:
        logger.warning("スナップショットデータがありません。")
        return

    # カテゴリーを件数合計で上位選出（「不明」は除外）
    cat_rows = conn.execute("""
        SELECT category, SUM(priced_count) as total
        FROM category_snapshot
        WHERE category != '不明'
        GROUP BY category
        ORDER BY total DESC
        LIMIT ?
    """, (TOP_CATEGORIES,)).fetchall()
    top_cats = [r[0] for r in cat_rows]

    # カテゴリー別時系列データ
    category_trends = {}
    for cat in top_cats:
        trend_rows = conn.execute("""
            SELECT snapshot_dt, priced_count, avg_buyback, avg_amazon, avg_spread, spread_positive
            FROM category_snapshot
            WHERE category = ?
            ORDER BY snapshot_dt
        """, (cat,)).fetchall()
        dt_map = {r[0]: r for r in trend_rows}

        category_trends[cat] = {
            "avg_buyback":      [round(dt_map[d][2]) if d in dt_map and dt_map[d][2] else None for d in snapshots],
            "avg_amazon":       [round(dt_map[d][3]) if d in dt_map and dt_map[d][3] else None for d in snapshots],
            "avg_spread":       [round(dt_map[d][4]) if d in dt_map and dt_map[d][4] else None for d in snapshots],
            "item_count":       [dt_map[d][1] if d in dt_map else 0 for d in snapshots],
            "spread_positive":  [dt_map[d][5] if d in dt_map else 0 for d in snapshots],
        }

    # 店舗別強み（カテゴリーごとの最終スナップショット時点での平均価格ランキング）
    last_dt = snapshots[-1]
    store_best = {}
    for cat in top_cats:
        s_rows = conn.execute("""
            SELECT store, avg_price, priced_count
            FROM store_snapshot
            WHERE snapshot_dt = ? AND category = ? AND avg_price IS NOT NULL
            ORDER BY avg_price DESC
        """, (last_dt, cat)).fetchall()
        if s_rows:
            top_price = s_rows[0][1]
            store_best[cat] = [
                {"store": r[0], "avg_price": round(r[1]), "count": r[2],
                 "ratio": round(r[1] / top_price * 100) if top_price > 0 else 0}
                for r in s_rows
            ]

    # 急騰・急落（直近2スナップショット間の変化率）
    recent_movers = {"up": [], "down": []}
    if len(snapshots) >= 2:
        prev_dt = snapshots[-2]
        for cat in top_cats:
            prev = conn.execute(
                "SELECT avg_buyback FROM category_snapshot WHERE snapshot_dt=? AND category=?",
                (prev_dt, cat)
            ).fetchone()
            curr = conn.execute(
                "SELECT avg_buyback FROM category_snapshot WHERE snapshot_dt=? AND category=?",
                (last_dt, cat)
            ).fetchone()
            if prev and curr and prev[0] and curr[0] and prev[0] > 0:
                change = curr[0] - prev[0]
                pct    = change / prev[0] * 100
                entry  = {"category": cat, "change": round(change), "pct": round(pct, 1),
                          "prev": round(prev[0]), "curr": round(curr[0])}
                if pct > 0:
                    recent_movers["up"].append(entry)
                else:
                    recent_movers["down"].append(entry)
        recent_movers["up"].sort(key=lambda x: -x["pct"])
        recent_movers["down"].sort(key=lambda x: x["pct"])

    # スプレッドランキング（最終スナップショット時点）
    spread_rows = conn.execute("""
        SELECT category, avg_spread, avg_buyback, avg_amazon, spread_positive, priced_count
        FROM category_snapshot
        WHERE snapshot_dt = ? AND category != '不明' AND avg_spread IS NOT NULL
        ORDER BY avg_spread DESC
        LIMIT ?
    """, (last_dt, TOP_SPREAD_CATS)).fetchall()
    spread_ranking = [
        {"category": r[0], "avg_spread": round(r[1]), "avg_buyback": round(r[2]) if r[2] else 0,
         "avg_amazon": round(r[3]) if r[3] else 0,
         "spread_positive": r[4], "priced_count": r[5]}
        for r in spread_rows
    ]

    payload = {
        "generated_at": __import__('datetime').datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "snapshot_count": len(snapshots),
        "snapshots": snapshots,
        "top_categories": top_cats,
        "category_trends": category_trends,
        "store_best": store_best,
        "spread_ranking": spread_ranking,
        "recent_movers": recent_movers,
        "last_snapshot": last_dt,
    }

    tmp = JSON_PATH.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
    tmp.replace(JSON_PATH)
    logger.info("trend_data.json を出力しました（スナップショット%d件、カテゴリー%d件）",
                len(snapshots), len(top_cats))


# ==================== メイン ====================
def main():
    conn = init_db()

    # 処理済みファイルを取得
    done = set(r[0] for r in conn.execute("SELECT filename FROM processed_files").fetchall())

    # 未処理CSVを日時順にソート
    csv_files = sorted(
        [f for f in CSV_DIR.glob("all_data_*.csv") if f.name not in done],
        key=lambda f: f.name
    )

    if not csv_files:
        logger.info("新しいCSVファイルはありません。JSON出力のみ実行します。")
    else:
        logger.info("未処理ファイル %d 件を処理します。", len(csv_files))
        for i, path in enumerate(csv_files, 1):
            snapshot_dt = parse_filename_dt(path.name)
            if not snapshot_dt:
                logger.warning("ファイル名から日時を取得できません: %s", path.name)
                continue
            logger.info("[%d/%d] %s (%s)", i, len(csv_files), path.name, snapshot_dt)
            process_csv(path, snapshot_dt, conn)

    export_json(conn)
    conn.close()
    logger.info("完了。")


if __name__ == "__main__":
    main()
