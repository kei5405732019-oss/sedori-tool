#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
catalog_crawler.py
--------------------
買取けんさく君の商品検索一覧（itemsearch/index.php）を start_no を増やしながら
ページングで全件巡回し、「品番 → カテゴリー・メーカー・買取価格」の対応表を
ローカルSQLite（catalog.db）に蓄積する。

kensakukun_watch.py の5分おき収集とは別に、1日1回程度の低頻度（cron想定）で
回すことを想定。サイト全体を舐めるためリクエスト数が多くなるので、
REQUEST_INTERVAL_SEC で間隔を空けて負荷をかけすぎないようにしている。

JANコードは商品詳細ページに表示されていないため取得できない。
カテゴリー・メーカー・新品/中古別の買取価格のみ取得する。

実行例:
  python3 catalog_crawler.py            # 全件クロール（時間がかかる）
  python3 catalog_crawler.py --max-pages 50   # 動作確認用に最初の50ページだけ

cron想定（毎日4:00、収集処理の少ない時間帯に）:
  0 4 * * * /usr/bin/python3 /home/kei5/kensakukun_watch/catalog_crawler.py >> /home/kei5/kensakukun_watch/catalog_crawler.log 2>&1
"""

import argparse
import logging
import re
import sqlite3
import sys
import time
from pathlib import Path
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

BASE_DIR = Path(__file__).resolve().parent
CATALOG_DB_PATH = BASE_DIR / "catalog.db"
LOG_PATH = BASE_DIR / "catalog_crawler.log"

LIST_URL = "https://kaitorikensakukun.com/itemsearch/index.php"
PAGE_SIZE = 10  # サイト側のページング単位（start_noの増分）
REQUEST_INTERVAL_SEC = 1.5  # 相手サーバーへの配慮。短くしすぎない
REQUEST_TIMEOUT = 15
MAX_EMPTY_PAGES_TO_STOP = 3  # 連続でこの回数空振りしたら終端とみなす

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/124.0 Safari/537.36"
    ),
    "Accept-Language": "ja,en;q=0.8",
}

PRICE_DIGIT_RE = re.compile(r"/images/(?:common/)?(\d|comma|yen)(?:_red)?\.(?:svg|png)$")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(LOG_PATH, encoding="utf-8"),
        logging.StreamHandler(sys.stdout),
    ],
)
logger = logging.getLogger(__name__)


def init_db() -> sqlite3.Connection:
    conn = sqlite3.connect(CATALOG_DB_PATH)
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS catalog (
            item_name TEXT NOT NULL,
            condition TEXT NOT NULL,
            top_category TEXT,
            sub_category TEXT,
            maker TEXT,
            price INTEGER,
            item_url TEXT,
            updated_at TEXT NOT NULL,
            PRIMARY KEY (item_name, condition)
        )
        """
    )
    conn.commit()
    return conn


def parse_price_from_images(td) -> int:
    """
    買取価格は数字1文字ごとに画像化されている（alt属性に数字が入っている）。
    例: ￥(yen) 5 5 , 8 0 0 -> 55800
    """
    digits = []
    for img in td.find_all("img"):
        alt = (img.get("alt") or "").strip()
        if alt.isdigit():
            digits.append(alt)
        # alt が無い/数字でない場合はsrcのファイル名から推測
        elif not alt:
            src = img.get("src", "")
            m = PRICE_DIGIT_RE.search(src)
            if m and m.group(1).isdigit():
                digits.append(m.group(1))
    if not digits:
        return None
    try:
        return int("".join(digits))
    except ValueError:
        return None


def fetch_page(start_no: int) -> str:
    params = {"start_no": start_no} if start_no > 0 else {}
    resp = requests.get(LIST_URL, params=params, headers=HEADERS, timeout=REQUEST_TIMEOUT)
    resp.raise_for_status()
    resp.encoding = resp.apparent_encoding or "utf-8"
    return resp.text


def parse_items(html: str):
    """
    1ページ分のHTMLから商品カードを抽出する。
    実際のマークアップは下記のようにクラス名付きの<dl>が項目ごとに並ぶ形式：

      <dl class="product"><dt>製品カテゴリー</dt><dd>テレビ・AV機器</dd></dl>
      <dl class="goods"><dt>商品カテゴリー</dt><dd>ブルーレイレコーダー</dd></dl>
      <dl class="maker"><dt>メーカー</dt><dd>パナソニック(Panasonic)</dd></dl>
      <dl class="name"><dt>商品名（型番）</dt><dd><a href="../items/103974.php">DMR-4TS204S</a></dd></dl>
      <dl class="state"><dt>新旧</dt><dd><span class="new">新品</span></dd></dl>
      <dl class="price"><dt>買取価格</dt><dd><a href="..."><img alt="5">...</a></dd></dl>

    1ページ内にこの6点セットが商品数ぶん繰り返されており、それぞれ
    find_all で取得した同インデックス同士が同じ商品を指す。
    """
    soup = BeautifulSoup(html, "html.parser")

    products = soup.find_all("dl", class_="product")
    goods = soup.find_all("dl", class_="goods")
    makers = soup.find_all("dl", class_="maker")
    names = soup.find_all("dl", class_="name")
    states = soup.find_all("dl", class_="state")
    prices = soup.find_all("dl", class_="price")

    n = min(len(products), len(goods), len(makers), len(names), len(states), len(prices))
    if n != len(names):
        logger.warning(
            "dlブロックの件数が揃っていません（product=%d goods=%d maker=%d name=%d state=%d price=%d）。"
            "件数が少ない方に合わせて処理します。",
            len(products), len(goods), len(makers), len(names), len(states), len(prices),
        )

    def dd_text(dl):
        dd = dl.find("dd")
        return dd.get_text(strip=True) if dd else None

    items = []
    for i in range(n):
        name_dd = names[i].find("dd")
        link = name_dd.find("a") if name_dd else None
        if not link:
            continue
        item_name = link.get_text(strip=True)
        item_url = link.get("href")
        if item_url:
            item_url = urljoin(LIST_URL, item_url)

        top_category = dd_text(products[i])
        sub_category = dd_text(goods[i])
        maker = dd_text(makers[i])

        cond_text = dd_text(states[i]) or ""
        if "新品" in cond_text:
            condition = "新品"
        elif "中古" in cond_text:
            condition = "中古"
        else:
            condition = cond_text or "不明"

        price_dd = prices[i].find("dd")
        price = parse_price_from_images(price_dd) if price_dd else None

        items.append(
            {
                "item_name": item_name,
                "condition": condition,
                "top_category": top_category,
                "sub_category": sub_category,
                "maker": maker,
                "price": price,
                "item_url": item_url,
            }
        )

    return items


def upsert_items(conn: sqlite3.Connection, items):
    if not items:
        return 0
    now = time.strftime("%Y-%m-%d %H:%M:%S")
    conn.executemany(
        """
        INSERT INTO catalog (item_name, condition, top_category, sub_category, maker, price, item_url, updated_at)
        VALUES (:item_name, :condition, :top_category, :sub_category, :maker, :price, :item_url, :updated_at)
        ON CONFLICT(item_name, condition) DO UPDATE SET
            top_category=excluded.top_category,
            sub_category=excluded.sub_category,
            maker=excluded.maker,
            price=excluded.price,
            item_url=excluded.item_url,
            updated_at=excluded.updated_at
        """,
        [{**it, "updated_at": now} for it in items],
    )
    conn.commit()
    return len(items)


def crawl(max_pages=None):
    conn = init_db()
    start_no = 0
    page_no = 0
    total_saved = 0
    empty_streak = 0

    while True:
        if max_pages is not None and page_no >= max_pages:
            logger.info("max_pages(%d)に達したため終了します。", max_pages)
            break

        try:
            html = fetch_page(start_no)
        except requests.RequestException as exc:
            logger.error("ページ取得失敗 start_no=%d: %s", start_no, exc)
            break

        items = parse_items(html)
        page_no += 1

        if not items:
            empty_streak += 1
            logger.info("start_no=%d: 商品なし（%d/%d回目の空振り）", start_no, empty_streak, MAX_EMPTY_PAGES_TO_STOP)
            if empty_streak >= MAX_EMPTY_PAGES_TO_STOP:
                logger.info("空振りが続いたため、カタログの終端と判断して終了します。")
                break
        else:
            empty_streak = 0
            saved = upsert_items(conn, items)
            total_saved += saved
            logger.info("start_no=%d: %d件取得・保存（累計%d件）", start_no, saved, total_saved)

        start_no += PAGE_SIZE
        time.sleep(REQUEST_INTERVAL_SEC)

    conn.close()
    logger.info("クロール完了。累計 %d 件をcatalog.dbに保存しました。", total_saved)


def main():
    parser = argparse.ArgumentParser(description="買取けんさく君 商品カタログクローラー")
    parser.add_argument(
        "--max-pages",
        type=int,
        default=None,
        help="動作確認用に取得するページ数の上限を指定（省略時は全件）",
    )
    args = parser.parse_args()
    crawl(max_pages=args.max_pages)


if __name__ == "__main__":
    main()
