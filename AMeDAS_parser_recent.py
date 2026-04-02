import requests
import zipfile
import pandas as pd
import os
import re
import time
import gzip
import logging
from datetime import datetime, timedelta
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# 設定 日誌 紀錄
LOG_LEVEL = os.getenv("AMEDAS_LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format='%(asctime)s %(levelname)s: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

ROOT = "./"


def load_station_sampling_config():
    sample_only = os.getenv("AMEDAS_SAMPLE_ONLY", "0") == "1"
    sample_stations = [s.strip() for s in os.getenv("AMEDAS_SAMPLE_STATIONS", "").split(",") if s.strip()]
    sample_limit = int(os.getenv("AMEDAS_SAMPLE_LIMIT", "5"))
    return sample_only, sample_stations, sample_limit


def filter_stations_for_debug(unique_sta_id):
    sample_only, sample_stations, sample_limit = load_station_sampling_config()
    if not sample_only:
        return unique_sta_id
    df = unique_sta_id.copy()
    if sample_stations:
        df = df[df["局ID"].isin(sample_stations)]
    if sample_limit > 0:
        df = df.head(sample_limit)
    logger.warning(f"DEBUG 抽樣模式啟用：本次僅處理 {len(df)} 個站點 -> {df['局ID'].tolist()}")
    return df


def dump_http_trace(station_id, year, month, payload, text):
    if os.getenv("AMEDAS_SAVE_HTTP_DUMP", "0") != "1":
        return
    dump_dir = os.getenv("AMEDAS_HTTP_DUMP_DIR", "debug_http")
    os.makedirs(dump_dir, exist_ok=True)
    base = f"{station_id}_{year}_{month}"
    payload_path = os.path.join(dump_dir, f"{base}_payload.txt")
    response_path = os.path.join(dump_dir, f"{base}_response_head.txt")
    with open(payload_path, "w", encoding="utf-8") as f:
        for k, v in payload.items():
            f.write(f"{k}={v}\n")
    with open(response_path, "w", encoding="utf-8") as f:
        f.write(text[:4000])


def build_session():
    session = requests.Session()
    # 避免環境中的代理設定造成連線被拒絕，導致整批更新中斷
    session.trust_env = False
    session.headers.update({
        "User-Agent": "Mozilla/5.0",
        "X-Requested-With": "XMLHttpRequest",
        "Origin": "https://www.data.jma.go.jp",
        "Referer": "https://www.data.jma.go.jp/risk/obsdl/",
    })
    retries = Retry(
        total=3,
        connect=3,
        read=3,
        backoff_factor=2,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET", "POST"],
    )
    adapter = HTTPAdapter(max_retries=retries)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session


def extract_sid(landing_text, session):
    patterns = [
        r'id="sid"\s+value="([^"]+)"',
        r'name="sid"\s+value="([^"]+)"',
        r"var\s+sid\s*=\s*'([^']+)'",
        r'var\s+sid\s*=\s*"([^"]+)"',
    ]
    for pattern in patterns:
        match = re.search(pattern, landing_text)
        if match:
            return match.group(1), f"html:{pattern}"

    cookie_sid = session.cookies.get("PHPSESSID")
    if cookie_sid:
        return cookie_sid, "cookie:PHPSESSID"
    return None, "not_found"

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
    r = requests.get(url, timeout=30)
    with open("ame_master.zip","wb") as f:
        f.write(r.content)
    with zipfile.ZipFile("ame_master.zip","r") as z:
        z.extractall("ame_master")
    csv_file = next(f for f in os.listdir("ame_master") if f.endswith(".csv"))
    amedas_df = read_csv_with_multiple_encodings(os.path.join("ame_master", csv_file))
    amedas_df["緯度"] = amedas_df.apply(lambda x: to_decimal(x["緯度(度)"], x["緯度(分)" ]), axis=1)
    amedas_df["経度"] = amedas_df.apply(lambda x: to_decimal(x["経度(度)"], x["経度(分)" ]), axis=1)
    amedas_df.to_csv(os.path.join("ame_master", csv_file), index=False)
    logger.info("AMeDAS 站點 列表 處理 完成")
    return amedas_df


def fetch_data_AMeDAS(station_id, year, month, end_day, session, sid):
    payload = {
        'stationNumList': f'["{station_id}"]',
        'aggrgPeriod':'9',
        # 依照 JMA 現行下載表單欄位調整
        'elementNumList':'[["201",""],["101",""],["503",""],["401",""],["501",""],["301",""],["612",""],["604",""],["605",""],["602",""],["601",""],["610",""],["703",""],["607",""],["704",""]]',
        'interAnnualType':'1',
        'ymdList':f'["{year}","{year}","{month}","{month}","1","{end_day}"]',
        'optionNumList':'[]',
        'downloadFlag':'true',
        'rmkFlag':'1',
        'disconnectFlag':'1',
        'youbiFlag':'0',
        'fukenFlag':'0',
        'kijiFlag':'0',
        'csvFlag':'1',
        'jikantaiFlag':'0',
        'jikantaiList':'[1,24]',
        'ymdLiteral':'1',
    }
    if sid:
        payload['PHPSESSID'] = sid
    try:
        resp = session.post('https://www.data.jma.go.jp/risk/obsdl/show/table', data=payload, timeout=60)
    except requests.RequestException as e:
        logger.error(f"{station_id} {year}-{month} POST 失敗：{e}")
        return ""
    logger.debug(f"{station_id} {year}-{month} POST 狀態碼: {resp.status_code}, bytes={len(resp.content)}")
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
    if "ダウンロードした時刻" not in text:
        dump_http_trace(station_id, year, month, payload, text)
    return text


def download_weather_data(unique_sta_id):
    start_time = datetime.now()
    time_limit = timedelta(hours=5, minutes=40)
    session = build_session()

    logger.info("取得 JMA 首頁 以 獲取 sid / ci_session")
    try:
        landing = session.get('https://www.data.jma.go.jp/risk/obsdl/', timeout=30)
    except requests.RequestException as e:
        logger.error(f"連線 JMA 首頁 失敗：{e}")
        return
    logger.debug(f"JMA 首頁狀態碼: {landing.status_code}, 長度: {len(landing.text)}")
    logger.info(f"首頁 cookies: {session.cookies.get_dict()}")
    sid, sid_source = extract_sid(landing.text, session)
    logger.info(f"sid 來源: {sid_source}")
    if sid:
        logger.info(f"sid: {sid}")
    else:
        logger.warning("未取得 sid，將改用 session cookie/無 sid 模式嘗試下載")
        logger.warning(f"目前 cookies: {session.cookies.get_dict()}")
        logger.error(f"首頁前 300 字元: {landing.text[:300]}")

    now = datetime.now()
    months = [(now.year, now.month)]
    if now.day <= 11:
        prev = (now.year - 1, 12) if now.month == 1 else (now.year, now.month - 1)
        months.append(prev)

    for _, row in filter_stations_for_debug(unique_sta_id).iterrows():
        if datetime.now() - start_time > time_limit:
            logger.info("時間到，結束下載")
            return
        station = row["局ID"]
        folder = os.path.join(ROOT, "weather_data", station)
        os.makedirs(folder, exist_ok=True)

        for y, m in months:
            now = datetime.now()
            end_day = min(max_day := (pd.Timestamp(y, m, 1) + pd.DateOffset(months=1) - pd.DateOffset(days=1)).day,
                          now.day if (y == now.year and m == now.month) else max_day)
            path = os.path.join(folder, f"{y}-{m}.csv.gz")
            logger.info(f"[{station}] 檢查 {y}-{m}，目標檔案: {path}")
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
            else:
                logger.info(f"{station} {y}-{m} 尚無本地檔案，將下載")

            if not need_download:
                continue

            logger.info(f"開始下載 {station} {y}-{m}")
            retries = 3
            while retries > 0:
                text = fetch_data_AMeDAS(station, y, m, end_day, session, sid)
                if "ダウンロードした時刻" in text:
                    with gzip.open(path, 'wt', encoding='cp932') as fw:
                        fw.write(text)
                    logger.info(f"{station} {y}-{m} 下載 成功")
                    break
                else:
                    retries -= 1
                    preview = text.splitlines()[:3]
                    logger.warning(f"{station} {y}-{m} 無更新 標記，剩 {retries} 次，回應前 3 行: {preview}")
                    time.sleep(10)
            if retries == 0:
                logger.error(f"{station} {y}-{m} 重試失敗，跳過")

if __name__ == "__main__":
    df = pd.read_csv(os.path.join(ROOT, 'stations/weather_stations.csv'))
    unique_ids = df.drop_duplicates(subset="局ID")
    download_weather_data(unique_ids)
