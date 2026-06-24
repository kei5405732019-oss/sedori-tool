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

# GAS連携（デプロイ後のURLを設定）
GAS_URL     = "https://script.google.com/macros/s/AKfycbzAKuTV0RB6Kl7dVD1vQMF2N8LKBLQ51UTSoIk3gnA9nMFJZcsvIZy3XxC_uHPQMLt2/exec"
GAS_ACCESS_KEY = "katuragiya"
GAS_TIMEOUT = 30

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
    # アイテム別スナップショット（JAN × スナップショット時点）
    conn.execute("""
        CREATE TABLE IF NOT EXISTS item_snapshot (
            jan          TEXT NOT NULL,
            snapshot_dt  TEXT NOT NULL,
            item_name    TEXT,
            category     TEXT,
            best_price   INTEGER,
            best_store   TEXT,
            amazon_price INTEGER,
            store_count  INTEGER NOT NULL DEFAULT 0,
            PRIMARY KEY (jan, snapshot_dt)
        )
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_item_jan ON item_snapshot(jan)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_item_dt ON item_snapshot(snapshot_dt)")
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

    # アイテム別バッファ（JAN単位、後でinsertmany）
    item_rows = []

    MAX_COLS = max(max(s[1], s[2], s[3], s[4] if s[4] else 0) for s in STORES) + 2

    try:
        with open(path, encoding="shift_jis", errors="replace", newline="") as f:
            reader = csv.reader(f)
            next(reader)  # ヘッダースキップ
            for row in reader:
                if len(row) < 10:
                    continue

                jan = (row[0] if len(row) > 0 else "").strip()
                if not jan:
                    continue

                # Amazon価格（カート価格を優先、なければ最低価格）
                amazon_cart = safe_int(row[2] if len(row) > 2 else "")
                amazon_min  = safe_int(row[6] if len(row) > 6 else "")
                amazon_price = amazon_cart if amazon_cart > 0 else amazon_min

                # 各店舗の買取価格・名前・カテゴリーを取得
                store_prices = {}
                store_names_per = {}
                categories   = []
                for store_name, name_col, price_col, dt_col, cat_col in STORES:
                    if price_col >= len(row):
                        continue
                    price = safe_int(row[price_col])
                    store_prices[store_name] = price
                    if name_col < len(row):
                        n = row[name_col].strip()
                        if n:
                            store_names_per[store_name] = n
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

                # 最良買取価格と最良店舗
                best_buyback = 0
                best_store = ""
                priced_store_count = 0
                for sname, p in store_prices.items():
                    if p > 0:
                        priced_store_count += 1
                        if p > best_buyback:
                            best_buyback = p
                            best_store = sname

                # 代表商品名（最良店舗のもの、なければ任意の店舗の名前）
                item_name = store_names_per.get(best_store) or next(iter(store_names_per.values()), "")

                # アイテム行を蓄積（買取価格が付いているもののみ）
                if best_buyback > 0:
                    item_rows.append((jan, snapshot_dt, item_name, category,
                                      best_buyback, best_store,
                                      amazon_price if amazon_price > 0 else None,
                                      priced_store_count))

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

    # アイテム別データの一括書き込み
    if item_rows:
        conn.executemany("""
            INSERT OR REPLACE INTO item_snapshot
            (jan, snapshot_dt, item_name, category, best_price, best_store, amazon_price, store_count)
            VALUES (?,?,?,?,?,?,?,?)
        """, item_rows)

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

    # JSON軽量化：スナップショットを間引き
    # 直近2週間=日次、2〜4週前=週次、それ以前=月次
    from datetime import datetime as _dt, timedelta as _td
    now = _dt.now()
    two_weeks_ago = (now - _td(days=14)).strftime("%Y-%m-%d %H:%M")
    four_weeks_ago = (now - _td(days=28)).strftime("%Y-%m-%d %H:%M")

    daily = []    # 直近2週
    weekly = {}   # 2〜4週（週番号→最新のスナップショット）
    monthly = {}  # それ以前（年月→最新のスナップショット）

    for s in snapshots:
        if s >= two_weeks_ago:
            daily.append(s)
        elif s >= four_weeks_ago:
            # ISO週番号でグルーピング
            d = _dt.strptime(s, "%Y-%m-%d %H:%M")
            wk = d.strftime("%Y-W%W")
            if wk not in weekly or s > weekly[wk]:
                weekly[wk] = s
        else:
            ym = s[:7]  # "YYYY-MM"
            if ym not in monthly or s > monthly[ym]:
                monthly[ym] = s

    snapshots_trimmed = sorted(set(list(monthly.values()) + list(weekly.values()) + daily))

    # カテゴリー別時系列データ（トリミング済み）
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
            "avg_buyback":      [round(dt_map[d][2]) if d in dt_map and dt_map[d][2] else None for d in snapshots_trimmed],
            "avg_amazon":       [round(dt_map[d][3]) if d in dt_map and dt_map[d][3] else None for d in snapshots_trimmed],
            "avg_spread":       [round(dt_map[d][4]) if d in dt_map and dt_map[d][4] else None for d in snapshots_trimmed],
            "item_count":       [dt_map[d][1] if d in dt_map else 0 for d in snapshots_trimmed],
            "spread_positive":  [dt_map[d][5] if d in dt_map else 0 for d in snapshots_trimmed],
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
        "snapshots": snapshots_trimmed,
        "top_categories": top_cats,
        "category_trends": category_trends,
        "store_best": store_best,
        "spread_ranking": spread_ranking,
        "recent_movers": recent_movers,
        "last_snapshot": last_dt,
        "item_movers": compute_item_movers(conn, snapshots),
        "watchlist": fetch_watchlist_with_trends(conn, snapshots),
    }

    tmp = JSON_PATH.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
    tmp.replace(JSON_PATH)
    logger.info("trend_data.json を出力しました（スナップショット%d件、カテゴリー%d件）",
                len(snapshots), len(top_cats))
    push_to_gas(payload)


def compute_item_movers(conn, snapshots):
    """
    各期間（1週・1ヶ月・3ヶ月）の価格上昇/下降アイテムTOP50を計算する。
    現在の最新スナップショットを基準とし、N日前の最も近いスナップショットと比較。
    """
    from datetime import datetime as _dt
    if len(snapshots) < 2:
        return {}
    last_dt = snapshots[-1]
    last_d = _dt.strptime(last_dt, "%Y-%m-%d %H:%M")

    # 期間ごとの「最も近いスナップショット」を選ぶ
    PERIODS = [("week", 7), ("month", 30), ("three_months", 90)]
    period_dts = {}
    for key, days in PERIODS:
        # last_d より days日前 に最も近いsnapshot
        target_iso = (last_d - __import__('datetime').timedelta(days=days)).strftime("%Y-%m-%d %H:%M")
        best = None
        best_diff = None
        for s in snapshots[:-1]:
            diff = abs((last_d - _dt.strptime(s, "%Y-%m-%d %H:%M")).days - days)
            if best_diff is None or diff < best_diff:
                best_diff = diff
                best = s
        if best:
            period_dts[key] = best

    result = {"last_snapshot": last_dt, "comparison": {}}

    # 最新スナップショットのアイテム情報
    curr_rows = conn.execute("""
        SELECT jan, item_name, category, best_price, best_store, store_count
        FROM item_snapshot
        WHERE snapshot_dt = ? AND best_price > 0
    """, (last_dt,)).fetchall()
    curr_map = {r[0]: r for r in curr_rows}

    for key, prev_dt in period_dts.items():
        prev_rows = conn.execute("""
            SELECT jan, best_price FROM item_snapshot
            WHERE snapshot_dt = ? AND best_price > 0
        """, (prev_dt,)).fetchall()
        prev_map = {r[0]: r[1] for r in prev_rows}

        diffs = []
        for jan, prev_price in prev_map.items():
            if jan not in curr_map:
                continue
            curr = curr_map[jan]
            curr_price = curr[3]
            change = curr_price - prev_price
            if change == 0:
                continue
            pct = (change / prev_price * 100) if prev_price > 0 else 0
            diffs.append({
                "jan":       jan,
                "name":      curr[1][:25] if curr[1] else "",
                "category":  curr[2],
                "store":     curr[4],
                "store_count": curr[5],
                "prev":      prev_price,
                "curr":      curr_price,
                "change":    change,
                "pct":       round(pct, 1),
            })

        # 上昇TOP30・下降TOP30（金額順）
        up = sorted([d for d in diffs if d["change"] > 0], key=lambda x: -x["change"])[:30]
        dn = sorted([d for d in diffs if d["change"] < 0], key=lambda x: x["change"])[:30]
        result["comparison"][key] = {
            "compared_to": prev_dt,
            "up": up,
            "down": dn,
        }

    return result


def fetch_watchlist_with_trends(conn, snapshots):
    """
    GASからウォッチリストを取得し、各JANの全スナップショット価格を抽出。
    名前・カテゴリーが未設定のエントリはDBから補完してGASも更新。
    """
    if not GAS_URL:
        return {"entries": []}
    try:
        import requests as _req
        resp = _req.get(f"{GAS_URL}?key={GAS_ACCESS_KEY}&action=watch_list", timeout=GAS_TIMEOUT)
        resp.raise_for_status()
        data = resp.json()
        entries = data.get("entries", [])
    except Exception as exc:
        logger.warning("ウォッチリスト取得失敗: %s", exc)
        return {"entries": []}

    if not entries:
        return {"entries": []}

    result = []
    recent_snaps = snapshots[-20:] if len(snapshots) > 20 else snapshots
    for ent in entries:
        jan = ent.get("jan")
        if not jan:
            continue
        rows = conn.execute("""
            SELECT snapshot_dt, best_price, amazon_price, best_store, item_name, category
            FROM item_snapshot WHERE jan = ? ORDER BY snapshot_dt
        """, (jan,)).fetchall()
        dt_map = {r[0]: r for r in rows}
        prices = [dt_map[s][1] if s in dt_map else None for s in recent_snaps]
        amazons = [dt_map[s][2] if s in dt_map else None for s in recent_snaps]
        latest = rows[-1] if rows else None

        # DB上の名前・カテゴリーを取得
        db_name = (latest[4] if latest else "") or ""
        db_category = (latest[5] if latest else "") or ""
        db_store = (latest[3] if latest else "") or ""
        db_price = latest[1] if latest else None

        # GAS側の名前・カテゴリーが空ならDBから補完してGASを更新
        gas_name = ent.get("name", "")
        gas_category = ent.get("category", "")
        if (not gas_name and db_name) or (not gas_category and db_category):
            try:
                import requests as _req2
                update_params = {
                    "key": GAS_ACCESS_KEY,
                    "action": "watch_update_info",
                    "jan": jan,
                    "name": db_name if not gas_name else "",
                    "category": db_category if not gas_category else "",
                }
                _req2.get(GAS_URL, params=update_params, timeout=GAS_TIMEOUT)
            except Exception:
                pass  # 更新失敗は無視

        result.append({
            "jan": jan,
            "name": gas_name or db_name,
            "category": gas_category or db_category,
            "current_price": db_price,
            "current_store": db_store,
            "current_amazon": latest[2] if latest else None,
            "target_price": ent.get("target_price"),
            "snapshot_count": len(rows),
            "memo": ent.get("memo", ""),
            "added": ent.get("added", ""),
            "prices": prices,
            "amazons": amazons,
        })
    return {"entries": result, "snapshots": recent_snaps}


def push_to_gas(payload: dict):
    """集計データをGASへPOSTする。
    item_moversはペイロードが大きいため別アクションで送信する。
    """
    if not GAS_URL:
        return
    import requests as _req

    # item_moversを分離（別シートへ別送）
    item_movers = payload.get("item_movers")
    base_payload = {k: v for k, v in payload.items() if k != "item_movers"}

    # ① ベースデータ送信
    try:
        body = {"key": GAS_ACCESS_KEY, "data": base_payload}
        resp = _req.post(GAS_URL, json=body, timeout=GAS_TIMEOUT)
        resp.raise_for_status()
        result = resp.json()
        if result.get("error"):
            logger.warning("GAS送信エラー: %s", result["error"])
        else:
            logger.info("GASへ送信しました（%d bytes, %d chunks）",
                        result.get("size", 0), result.get("chunks", 0))
    except Exception as exc:
        logger.warning("GAS送信失敗: %s", exc)

    # ② item_movers 別送
    if item_movers:
        try:
            im_body = {
                "key": GAS_ACCESS_KEY,
                "action": "update_item_movers",
                "item_movers": item_movers,
            }
            resp2 = _req.post(GAS_URL, json=im_body, timeout=GAS_TIMEOUT)
            resp2.raise_for_status()
            result2 = resp2.json()
            if result2.get("error"):
                logger.warning("item_movers GAS送信エラー: %s", result2["error"])
            else:
                logger.info("item_movers GASへ送信しました（%d bytes, %d chunks）",
                            result2.get("size", 0), result2.get("chunks", 0))
        except Exception as exc:
            logger.warning("item_movers GAS送信失敗: %s", exc)


# ==================== メイン ====================
def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--rebuild-items", action="store_true",
                        help="既存80CSVを再処理してitem_snapshotテーブルを構築")
    args = parser.parse_args()

    conn = init_db()

    if args.rebuild_items:
        logger.info("--rebuild-items: 既存全CSVを再処理します。")
        # processed_filesを一度クリアして全件処理し直す
        conn.execute("DELETE FROM processed_files")
        # item_snapshotもクリア（古いデータがあれば）
        conn.execute("DELETE FROM item_snapshot")
        conn.commit()

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
