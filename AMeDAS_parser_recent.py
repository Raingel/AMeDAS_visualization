import requests
import zipfile
import pandas as pd
import os
import re
import time
import gzip
from datetime import datetime, timedelta

# Function to convert 緯度(度) 緯度(分) 経度(度) 経度(分) to decimal
def to_decimal(d, m):
    return d + m / 60

ROOT = "./"

# Function to try reading the CSV with multiple encodings
def read_csv_with_multiple_encodings(file_path, encodings=['cp932', 'utf-8', 'shift_jis', 'euc-jp']):
    for encoding in encodings:
        try:
            df = pd.read_csv(file_path, encoding=encoding)
            print(f"Successfully read the file with encoding: {encoding}")
            return df
        except UnicodeDecodeError as e:
            print(f"Failed to read with encoding {encoding}: {e}")
    raise ValueError("Failed to read the file with all provided encodings")

# Function to download latest official AMeDAS station list from JMA
def download_amedas_station_list():
    AMeDAS_STA_list = "https://www.jma.go.jp/jma/kishou/know/amedas/ame_master.zip"
    r = requests.get(AMeDAS_STA_list)
    with open("ame_master.zip", "wb") as code:
        code.write(r.content)
    with zipfile.ZipFile("ame_master.zip", "r") as z:
        z.extractall("ame_master")

    # Get the only CSV file in the extracted folder
    for file in os.listdir("ame_master"):
        if file.endswith(".csv"):
            AMeDAS_STA_file = file
            AMeDAS_STA_df = read_csv_with_multiple_encodings("ame_master/" + AMeDAS_STA_file)
            break

    AMeDAS_STA_df["緯度"] = AMeDAS_STA_df.apply(lambda x: to_decimal(x["緯度(度)"], x["緯度(分)"]), axis=1)
    AMeDAS_STA_df["経度"] = AMeDAS_STA_df.apply(lambda x: to_decimal(x["経度(度)"], x["経度(分)"]), axis=1)
    AMeDAS_STA_df.to_csv("ame_master/" + AMeDAS_STA_file, index=False)
    return AMeDAS_STA_df

# Function to get station data from JMA
def get_sta_from_JMA(pd="00"):
    cookies = {
        'AWSALB': 'osx6uR/c6KwcyMebiovRy3gAW+4aZLfcQPtU+6wJWwUnFm7qGQ3i1GXSVcIjBxrIJzLBkNrBn7CjRX6ixdUNbq1yVKy4/YrUzoF+GdpaoZYGXvHTkpFaB+WhoTB6',
        'AWSALBCORS': 'osx6uR/c6KwcyMebiovRy3gAW+4aZLfcQPtU+6wJWwUnFm7qGQ3i1GXSVcIjBxrIJzLBkNrBn7CjRX6ixdUNbq1yVKy4/YrUzoF+GdpaoZYGXvHTkpFaB+WhoTB6',
    }
    headers = {
        'Accept': 'text/html, */*; q=0.01',
        'Accept-Language': 'ja-JP,ja;q=0.9,zh-TW;q=0.8,zh;q=0.7,en-US;q=0.6,en;q=0.5',
        'Connection': 'keep-alive',
        'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
        'Origin': 'https://www.data.jma.go.jp',
        'Referer': 'https://www.data.jma.go.jp/risk/obsdl/index.php',
        'Sec-Fetch-Dest': 'empty',
        'Sec-Fetch-Mode': 'cors',
        'Sec-Fetch-Site': 'same-origin',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
        'X-Requested-With': 'XMLHttpRequest',
        'sec-ch-ua': '"Chromium";v="124", "Google Chrome";v="124", "Not-A.Brand";v="99"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
    }
    data = {
        'pd': pd,
    }
    response = requests.post('https://www.data.jma.go.jp/gmd/risk/obsdl/top/station', cookies=cookies, headers=headers, data=data)
    response.encoding = response.apparent_encoding
    return response.text

# Function to fetch AMeDAS data
def fetch_data_AMeDAS(station_id, year, month, session, sid):
    max_day = (pd.Timestamp(year, month, 1) + pd.DateOffset(months=1) - pd.DateOffset(days=1)).day
    data = {
        'stationNumList': f'["{station_id}"]',
        'aggrgPeriod': '9',
        'elementNumList': '[["201",""],["101",""],["610",""],["703",""],["704",""],["607",""],["601",""],["602",""],["605",""],["301",""],["401",""],["501",""],["503",""]]',
        'interAnnualFlag': '1',
        'ymdList': f'["{year}","{year}","{month}","{month}","1","{max_day}"]',
        'optionNumList': '[]',
        'downloadFlag': 'true',
        'rmkFlag': '1',
        'disconnectFlag': '1',
        'youbiFlag': '0',
        'fukenFlag': '0',
        'kijiFlag': '0',
        'huukouFlag': '0',
        'csvFlag': '1',
        'jikantaiFlag': '0',
        'jikantaiList': '[1,24]',
        'ymdLiteral': '1',
        'PHPSESSID': sid,
    }
    response = session.post('https://www.data.jma.go.jp/risk/obsdl/show/table', data=data)
    response.encoding = response.apparent_encoding
    return response.text

# Function to download data based on the current date
def download_weather_data(unique_sta_id):
    session = requests.Session()
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
        "Accept": "text/html, */*; q=0.01",
        "Accept-Encoding": "gzip, deflate, br, zstd",
        "Accept-Language": "ja-JP,ja;q=0.9",
        "Connection": "keep-alive",
        "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
        "Origin": "https://www.data.jma.go.jp",
        "Referer": "https://www.data.jma.go.jp/risk/obsdl/index.php",
        "Sec-Ch-Ua": '"Chromium";v="124", "Google Chrome";v="124", "Not-A.Brand";v="99"',
        "Sec-Ch-Ua-Mobile": "?0",
        "Sec-Ch-Ua-Platform": "Windows",
        "Sec-Fetch-Dest": "empty",
        "Sec-Fetch-Mode": "cors",
        "Sec-Fetch-Site": "same-origin",
        "X-Requested-With": "XMLHttpRequest"
    }
    session.headers.update(headers)
    post_url = "https://www.data.jma.go.jp/risk/obsdl/index.php"
    response = session.get(post_url, headers=headers)
    response.encoding = response.apparent_encoding
    sid = re.search(r'<input type="hidden" id="sid" value="(.*?)"', response.text).group(1)

    current_date = datetime.now()
    current_year = current_date.year
    current_month = current_date.month
    previous_month = current_month - 1 if current_month > 1 else 12
    previous_year = current_year if current_month > 1 else current_year - 1

    # Determine if we need to download the previous month's data
    months_to_download = [(current_year, current_month)]
    if current_date.day <= 11:
        months_to_download.append((previous_year, previous_month))

    for index, row in unique_sta_id.iterrows():
        time.sleep(5)
        station_id = row["局ID"]
        os.makedirs(f'{ROOT}/weather_data/{station_id}', exist_ok=True)
        for year, month in months_to_download:
            path = f'{ROOT}/weather_data/{station_id}/{year}-{month}.csv.gz'
            if os.path.exists(path):
                with gzip.open(path, 'rt', encoding="utf8", errors='ignore') as f:
                    if "ダウンロードした時刻" in f.read():
                        #print(f"Skipping data for {station_id} in {year}-{month}")
                        #continue
                        1
            print(f"Downloading data for {station_id} in {year}-{month}")
            retries = 3
            while retries > 0:
                try:
                    data = fetch_data_AMeDAS(station_id, year, month, session, sid)
                    if "ダウンロードした時刻" in data:
                        with gzip.open(path, 'wt', encoding="utf8") as f:
                            f.write(data)
                        print(f"Successfully downloaded data for {station_id} in {year}-{month}")
                        break
                except Exception as e:
                    print(f"Error downloading data for {station_id} in {year}-{month}: {e}")
                    retries -= 1
                    if retries > 0:
                        print(f"Retrying... ({3 - retries} of 3 retries)")
                    else:
                        print(f"Failed to download data for {station_id} in {year}-{month} after 3 retries")
                time.sleep(10)

# Main execution
AMeDAS_STA_df = download_amedas_station_list()
perfecture = get_sta_from_JMA(pd="00")
perfecture_list = re.findall(r'<div class="prefecture" id="pr(\d+)">(.+?)<input type="hidden" name="prid" value="\d+">', perfecture)

pool = []
for prid, prname in perfecture_list:
    sta_list_response = get_sta_from_JMA(pd=prid)
    pattern = r'<div style="width:100%; height:100%;" class="station"(.*?)<input type="hidden" name="kansoku"'
    matches = re.findall(pattern, sta_list_response, re.DOTALL)
    for m in matches:
        stname = re.search(r'name="stname" value="(.*?)"', m).group(1)
        stid = re.search(r'name="stid" value="(.*?)"', m).group(1)
        lat = re.search(r'緯：(.*?)度(.*?)分', m).groups()
        lat = to_decimal(float(lat[0]), float(lat[1]))
        lon = re.search(r'経：(.*?)度(.*?)分', m).groups()
        lon = to_decimal(float(lon[0]), float(lon[1]))
        pool.append([stid, stname, lat, lon])

JMA_STA_df = pd.DataFrame(pool, columns=["局ID", "局名", "緯度", "経度"])
JMA_STA_df["局名"] = JMA_STA_df["局名"].str.replace(" ", "")
AMeDAS_STA_df["局名"] = AMeDAS_STA_df["観測所名"].str.replace(" ", "")
merged = pd.merge(JMA_STA_df, AMeDAS_STA_df, on="局名", how="inner")
merged.to_csv(f"{ROOT}stations/merged_sta_list.csv", index=False)

merged = pd.read_csv(f"{ROOT}stations/merged_sta_list.csv")
unique_sta_id = merged.drop_duplicates(subset="局ID")

download_weather_data(unique_sta_id)
