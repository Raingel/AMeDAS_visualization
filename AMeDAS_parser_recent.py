# -*- coding: utf-8 -*-
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
    format='%(Y-%m-%d %H:%M:%S) %(levelname)s: %(message)s'
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

def download_amedas_station_list():
    url = "https://www.jma.go.jp/jma/kishou/know/amedas/ame_master.zip"
    logger.info("下載 AMeDAS 站點 列表")
    r = requests.get(url)
    with open("ame_master.zip","wb") as f:
        f.write(r.content)
    with zipfile.ZipFile("ame_master.zip","r") as z:
        z.extractall("ame_master")
    csv_file = next(f for f in os.listdir("ame_master") if f.endswith(".csv"))
    amedas_df = read_csv_with_multiple_encodings(os.path.join("ame_master", csv_file))
    amedas_df["緯度"] = amedas_df.apply(lambda x: to_decimal(x["緯度(度)"],x["緯度(分)"]),axis=1)
    amedas_df["経度"] = amedas_df.apply(lambda x: to_decimal(x["経度(度)"],x["経度(分)"]),axis=1)
    amedas_df.to_csv(os.path.join("ame_master",csv_file), index=False)
    logger.info("AMeDAS 站點 列表 處理 完成")
    return amedas_df

def get_sta_from_JMA(pd="00"):
    cookies = {
        'AWSALB': 'osx6uR/c6KwcyMebiovRy3gAW+4aZLfcQPtU+6wJWwUnFm7qGQ3i1GXSVcIjBxrIJzLBkNrBn7CjRX6ixdUNbq1yVKy4/YrUzoF+GdpaoZYGXvHTkpFaB+WhoTB6',
        'AWSALBCORS': 'osx6uR/c6KwcyMebiovRy3gAW+4aZLfcQPtU+6wJWwUnFm7qGQ3i1GXSVcIjBxrIJzLBkNrBn7CjRX6ixdUNbq1yVKy4/YrUzoF+GdpaoZYGXvHTkpFaB+WhoTB6',
    }
    headers = {
        'Accept':'text/html, */*; q=0.01',
        'Accept-Language':'ja-JP,ja;q=0.9,zh-TW;q=0.8,zh;q=0.7,en-US;q=0.6,en;q=0.5',
        'Connection':'keep-alive',
        'Content-Type':'application/x-www-form-urlencoded; charset=UTF-8',
        'Origin':'https://www.data.jma.go.jp',
        'Referer':'https://www.data.jma.go.jp/risk/obsdl/index.php',
        'Sec-Fetch-Dest':'empty',
        'Sec-Fetch-Mode':'cors',
        'Sec-Fetch-Site':'same-origin',
        'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
        'X-Requested-With':'XMLHttpRequest',
        'sec-ch-ua':'"Chromium";v="124", "Google Chrome";v="124", "Not-A.Brand";v="99"',
        'sec-ch-ua-mobile':'?0',
        'sec-ch-ua-platform':'"Windows"',
    }
    data = {'pd':pd}
    response = requests.post(
        'https://www.data.jma.go.jp/gmd/risk/obsdl/top/station',
        cookies=cookies,
        headers=headers,
        data=data
    )
    response.encoding = response.apparent_encoding
    return response.text

def fetch_data_AMeDAS(station_id, year, month, session, sid):
    max_day = (pd.Timestamp(year, month,1)+pd.DateOffset(months=1)-pd.DateOffset(days=1)).day
    data = {
        'stationNumList':f'["{station_id}"]',
        'aggrgPeriod':'9',
        'elementNumList':'[["201",""],["101",""],["610",""],["703",""],["704",""],["607",""],["601",""],["602",""],["605",""],["301",""],["401",""],["501",""],["503",""]]',
        'interAnnualFlag':'1',
        'ymdList':f'["{year}","{year}","{month}","{month}","1","{max_day}"]',
        'optionNumList':'[]',
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
    response = session.post(
        'https://www.data.jma.go.jp/risk/obsdl/show/table',
        data=data
    )
    response.encoding = response.apparent_encoding
    return response.text

def download_weather_data(unique_sta_id):
    start_time = datetime.now()
    time_limit = timedelta(hours=5, minutes=40)

    session = requests.Session()
    session.headers.update({
        "User-Agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
        "Accept":"text/html, */*; q=0.01",
        "Accept-Encoding":"gzip, deflate, br, zstd",
        "Accept-Language":"ja-JP,ja;q=0.9",
        "Connection":"keep-alive",
        "Content-Type":"application/x-www-form-urlencoded; charset=UTF-8",
        "Origin":"https://www.data.jma.go.jp",
        "Referer":"https://www.data.jma.go.jp/risk/obsdl/index.php",
        "Sec-Ch-Ua":'"Chromium";v="124", "Google Chrome";v="124", "Not-A.Brand";v="99"',
        "Sec-Ch-Ua-Mobile":"?0",
        "Sec-Ch-Ua-Platform":"Windows",
        "Sec-Fetch-Dest":"empty",
        "Sec-Fetch-Mode":"cors",
        "Sec-Fetch-Site":"same-origin",
        "X-Requested-With":"XMLHttpRequest",
    })

    # 取得 session id
    logger.info("取得 JMA 首頁 以 取得 session id")
    post_url = "https://www.data.jma.go.jp/risk/obsdl/index.php"
    response = session.get(post_url)
    response.encoding = response.apparent_encoding
    sid_match = re.search(r'<input type="hidden" id="sid" value="(.*?)"', response.text)
    if not sid_match:
        logger.error("未 能 取得 session id")
        return
    sid = sid_match.group(1)
    logger.info(f"session id: {sid}")

    # 確定 要 下載 的 月份
    now = datetime.now()
    current_year, current_month = now.year, now.month
    prev_year, prev_month = (current_year, current_month-1) if current_month>1 else (current_year-1,12)
    months_to_download = [(current_year, current_month)]
    if now.day <=11:
        months_to_download.append((prev_year, prev_month))

    for _, row in unique_sta_id.iterrows():
        # 時間 限制 檢查
        if datetime.now() - start_time > time_limit:
            logger.info("已 達 5 小時 40 分 時間 上限，結束 下載")
            return

        station_id = row["局ID"]
        folder = os.path.join(ROOT, "weather_data", station_id)
        os.makedirs(folder, exist_ok=True)

        for year, month in months_to_download:
            check_time = datetime.now()
            path = os.path.join(folder, f"{year}-{month:02d}.csv.gz")

            if os.path.exists(path):
                logger.info(f"找到 檔案 {path}")
                with gzip.open(path, 'rt', encoding='utf8', errors='ignore') as f:
                    content = f.read()
                m = re.search(r"ダウンロードした時刻：(\d{4}/\d{2}/\d{2} \d{2}:\d{2}:\d{2})", content)
                if m:
                    dl_time = datetime.strptime(m.group(1), "%Y/%m/%d %H:%M:%S")
                    if check_time - dl_time < timedelta(hours=24):
                        logger.info(f"{station_id} {year}-{month:02d} 在 24 小時 內 已 更新，跳過")
                        continue
                    else:
                        logger.info(f"{station_id} {year}-{month:02d} 超過 24 小時，重新 下載")
                else:
                    logger.info(f"{station_id} {year}-{month:02d} 無 更新 時刻，重新 下載")
            else:
                logger.info(f"{station_id} {year}-{month:02d} 無 檔案，開始 下載")

            # 下載 並 重試 機制
            retries = 3
            while retries > 0:
                try:
                    html = fetch_data_AMeDAS(station_id, year, month, session, sid)
                    if "ダウンロードした時刻" in html:
                        with gzip.open(path, 'wt', encoding='utf8') as f_out:
                            f_out.write(html)
                        logger.info(f"{station_id} {year}-{month:02d} 下載 成功")
                        break
                    else:
                        logger.warning(f"{station_id} {year}-{month:02d} 下載 內容 無 更新 標記")
                except Exception as e:
                    retries -= 1
                    logger.error(f"{station_id} {year}-{month:02d} 下載 失敗：{e}，剩餘 重試 {retries}")
                time.sleep(10)

if __name__ == "__main__":
    UPDATE_WEATHER_STA = False

    if UPDATE_WEATHER_STA:
        amedas_df = download_amedas_station_list()
        prefecture = get_sta_from_JMA(pd="00")
        prefecture_list = re.findall(
            r'<div class="prefecture" id="pr(\d+)">(.+?)<input type="hidden" name="prid" value="\d+">',
            prefecture
        )
        pool = []
        for prid, _ in prefecture_list:
            resp = get_sta_from_JMA(pd=prid)
            pattern = r'<div style="width:100%; height:100%;" class="station"(.*?)<input type="hidden" name="kansoku"'
            matches = re.findall(pattern, resp, re.DOTALL)
            for m in matches:
                stname = re.search(r'name="stname" value="(.*?)"', m).group(1).strip()
                stid  = re.search(r'name="stid"  value="(.*?)"', m).group(1)
                lat_d, lat_m = re.search(r'緯：(.*?)度(.*?)分', m).groups()
                lon_d, lon_m = re.search(r'経：(.*?)度(.*?)分', m).groups()
                pool.append([
                    stid,
                    stname,
                    to_decimal(float(lat_d), float(lat_m)),
                    to_decimal(float(lon_d), float(lon_m))
                ])
        JMA_STA_df = pd.DataFrame(pool, columns=["局ID","局名","緯度","経度"])
        JMA_STA_df["局名"] = JMA_STA_df["局名"].str.replace(" ","")
        amedas_df["局名"] = amedas_df["観測所名"].str.replace(" ","")
        amedas_df = amedas_df[['都府県振興局','観測所番号','種類','局名','ｶﾀｶﾅ名']]
        combined_df = pd.merge(JMA_STA_df, amedas_df, how="left", on="局名").dropna(subset=["局ID"])
        os.makedirs(os.path.dirname(f"{ROOT}stations/weather_stations.csv"), exist_ok=True)
        combined_df.to_csv(f"{ROOT}stations/weather_stations.csv", index=False)

    combined_df   = pd.read_csv(f"{ROOT}stations/weather_stations.csv")
    unique_sta_id = combined_df.drop_duplicates(subset="局ID")
    download_weather_data(unique_sta_id)
