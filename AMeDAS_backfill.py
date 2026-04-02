import os
import gzip
import re
import logging
import pandas as pd
from datetime import datetime

from AMeDAS_parser_recent import (
    build_session,
    extract_sid,
    fetch_data_AMeDAS,
    is_probable_no_data_html,
)


LOG_LEVEL = os.getenv("AMEDAS_LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s %(levelname)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

ROOT = "./"


def iter_months(start_year: int, start_month: int, end_year: int, end_month: int):
    y, m = start_year, start_month
    while (y, m) <= (end_year, end_month):
        yield y, m
        m += 1
        if m > 12:
            y += 1
            m = 1


def should_skip_existing(path: str) -> bool:
    if not os.path.exists(path):
        return False
    try:
        with gzip.open(path, "rt", encoding="cp932", errors="ignore") as f:
            head = f.read(512)
        return "ダウンロードした時刻" in head
    except Exception:
        return False


def main():
    now = datetime.now()
    start_ym = os.getenv("AMEDAS_BACKFILL_START", "2025-10")
    start_year, start_month = map(int, start_ym.split("-"))

    stations_df = pd.read_csv(os.path.join(ROOT, "stations/weather_stations.csv"))
    unique_sta = stations_df.drop_duplicates(subset="局ID")

    session = build_session()
    landing = session.get("https://www.data.jma.go.jp/risk/obsdl/", timeout=30)
    sid, sid_source = extract_sid(landing.text, session)
    logger.info(f"sid source: {sid_source}, cookies={session.cookies.get_dict()}")

    months = list(iter_months(start_year, start_month, now.year, now.month))
    logger.info(f"backfill month range: {months[0]} -> {months[-1]} ({len(months)} months)")

    for _, row in unique_sta.iterrows():
        station = row["局ID"]
        folder = os.path.join(ROOT, "weather_data", station)
        os.makedirs(folder, exist_ok=True)

        for y, m in months:
            path = os.path.join(folder, f"{y}-{m}.csv.gz")
            if should_skip_existing(path):
                continue

            month_last_day = (pd.Timestamp(y, m, 1) + pd.DateOffset(months=1) - pd.DateOffset(days=1)).day
            if y == now.year and m == now.month:
                prev = datetime(y, m, 1) - pd.Timedelta(days=1)
                start_y, start_m = prev.year, prev.month
                end_day = max(1, now.day - 1)
            else:
                start_y, start_m = y, m
                end_day = month_last_day

            logger.info(f"[{station}] backfill {y}-{m}, query={start_y}/{start_m}/1 -> {y}/{m}/{end_day}")
            text = fetch_data_AMeDAS(start_y, start_m, y, m, end_day, station, session, sid)
            if "ダウンロードした時刻" in text:
                with gzip.open(path, "wt", encoding="cp932") as fw:
                    fw.write(text)
                logger.info(f"[{station}] {y}-{m} updated")
            elif is_probable_no_data_html(text):
                logger.info(f"[{station}] {y}-{m} no-data html, skip")
            else:
                logger.warning(f"[{station}] {y}-{m} unexpected response, skip")


if __name__ == "__main__":
    main()
