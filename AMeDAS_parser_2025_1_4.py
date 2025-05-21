import requests
import zipfile
import pandas as pd
import os
import re
import time
import gzip
import logging
from datetime import datetime, timedelta

# 設定 日誌 紀錄
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

ROOT = "./"

def to_decimal(d, m):
    return d + m / 60


def read_csv_with_multiple_encodings(file_path, encodings=['cp932','utf-8','shift_jis','euc-jp']):
    for encoding in encodings:
        try:
            df = pd.read_csv(file_path, encoding=encoding)
            logger.info(f"成功 使用 編碼 {encoding} 讀取 檔案")
            return df
        except UnicodeDecodeError as e:
            logger.warning(f"使用 編碼 {encoding} 讀取 失敗：{e}")
    raise ValueError("無法 使用 提供 之 編碼 讀取 檔案")


def fetch_data_AMeDAS(station_id, year, month, session, sid):
    max_day = (pd.Timestamp(year, month, 1) + pd.DateOffset(months=1) - pd.DateOffset(days=1)).day
    payload = {
        'stationNumList': f'["{station_id}"]',
        'aggrgPeriod':'9',
        'elementNumList':'[["201",""],["101",""],["610",""],["703",""],["704",""],["607",""],["601",""],["602",""],["605",""],["301",""],["401",""],["501",""],["503",""]]',
        'interAnnualFlag':'1',
        'ymdList':f'["{year}","{year}","{month}","{month}","1","{max_day}"]',
        'downloadFlag':'true',
        'rmkFlag':'1',
        'disconnectFlag':'1',
        'youbiFlag':'0',
        'fukenFlag':'0',
        'kijiFlag':'0',
        'huukouFlag':'0',
        'csvFlag':'1',
        'jikantaiFlag':'0',
        'jikantaiList':'[1,24]',
        'ymdLiteral':'1',
        'PHPSESSID':sid,
    }
    resp = session.post('https://www.data.jma.go.jp/risk/obsdl/show/table', data=payload)
    raw = resp.content
    try:
        text = raw.decode('cp932')
        logger.debug(f"{station_id} 解碼: cp932")
    except UnicodeDecodeError:
        try:
            text = raw.decode('utf-8')
            logger.debug(f"{station_id} 解碼: utf-8")
        except Exception:
            text = raw.decode('cp932', errors='ignore')
            logger.warning(f"{station_id} 強制 cp932 解碼 (忽略錯誤)")
    return text


def download_weather_data(unique_sta_id):
    start_time = datetime.now()
    time_limit = timedelta(hours=5, minutes=40)
    session = requests.Session()
    session.headers.update({"User-Agent":"Mozilla/5.0","X-Requested-With":"XMLHttpRequest"})

    logger.info("取得 JMA 首頁 以 獲取 sid")
    landing = session.get('https://www.data.jma.go.jp/risk/obsdl/index.php')
    match = re.search(r'id="sid" value="(.*?)"', landing.text)
    if not match:
        logger.error("未 能 取得 sid")
        return
    sid = match.group(1)
    logger.info(f"sid: {sid}")

    # 指定 重新 下載 今年 1-4 月
    year = datetime.now().year
    months = [(year, m) for m in range(1, 5)]

    for _, row in unique_sta_id.iterrows():
        if datetime.now() - start_time > time_limit:
            logger.info("時間到，結束下載")
            return
        station = row["局ID"]
        folder = os.path.join(ROOT, "weather_data", station)
        os.makedirs(folder, exist_ok=True)

        for y, m in months:
            path = os.path.join(folder, f"{y}-{m}.csv.gz")
            need_download = True
            if os.path.exists(path):
                logger.info(f"找到 {path}")
                with gzip.open(path, 'rb') as f:
                    raw = f.read()
                try:
                    content = raw.decode('cp932')
                except Exception:
                    content = raw.decode('utf-8', errors='ignore')
                mtime = re.search(r"ダウンロードした時刻：(\d{4}/\d{2}/\d{2} \d{2}:\d{2}:\d{2})", content)
                if mtime:
                    dt = datetime.strptime(mtime.group(1), "%Y/%m/%d %H:%M:%S")
                    if datetime.now() - dt < timedelta(hours=24):
                        logger.info(f"{station} {y}-{m} 24H 內已更新，跳過")
                        need_download = False
                    else:
                        logger.info(f"{station} {y}-{m} 超過24H，重新下載")
                else:
                    logger.info(f"{station} {y}-{m} 無更新時間，重新下載")

            if not need_download:
                continue

            logger.info(f"開始下載 {station} {y}-{m}")
            retries = 3
            while retries > 0:
                text = fetch_data_AMeDAS(station, y, m, session, sid)
                if "ダウンロードした時刻" in text:
                    with gzip.open(path, 'wt', encoding='cp932') as fw:
                        fw.write(text)
                    logger.info(f"{station} {y}-{m} 下載 成功")
                    break
                else:
                    retries -= 1
                    preview = text.splitlines()[:3]
                    logger.warning(f"{station} {y}-{m} 無更新 標記，剩 {retries} 次，前三行: {preview}")
                    time.sleep(10)
            if retries == 0:
                logger.error(f"{station} {y}-{m} 重試失敗，跳過")

if __name__ == "__main__":
    df = pd.read_csv(os.path.join(ROOT, 'stations/weather_stations.csv'))
    unique_ids = df.drop_duplicates(subset="局ID")
    download_weather_data(unique_ids)
