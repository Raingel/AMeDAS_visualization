# %%
import requests
import zipfile
import pandas as pd
import os
import re
#Convert 緯度(度) 緯度(分) 経度(度) 経度(分) to decimal
def to_decimal(d, m):
    return d + m/60
ROOT = "./"

# %%
#Download latest offical AMeDAS station list from JMA
AMeDAS_STA_list = "https://www.jma.go.jp/jma/kishou/know/amedas/ame_master.zip"
r = requests.get(AMeDAS_STA_list)
with open("ame_master.zip", "wb") as code:
    code.write(r.content)
with zipfile.ZipFile("ame_master.zip", "r") as z:
    z.extractall("ame_master")

#Get the only csv file in the extracted folder
for file in os.listdir("ame_master"):
    if file.endswith(".csv"):
        AMeDAS_STA_file = file
        AMeDAS_STA_df = pd.read_csv("ame_master/"+AMeDAS_STA_file, encoding="cp932")
        break

AMeDAS_STA_df["緯度"] = AMeDAS_STA_df.apply(lambda x: to_decimal(x["緯度(度)"], x["緯度(分)"]), axis=1)
AMeDAS_STA_df["経度"] = AMeDAS_STA_df.apply(lambda x: to_decimal(x["経度(度)"], x["経度(分)"]), axis=1)
AMeDAS_STA_df.to_csv("ame_master/"+AMeDAS_STA_file, index=False)

# %%
def get_sta_from_JMA (pd = "00"):
    cookies = {
    'AWSALB': 'osx6uR/c6KwcyMebiovRy3gAW+4aZLfcQPtU+6wJWwUnFm7qGQ3i1GXSVcIjBxrIJzLBkNrBn7CjRX6ixdUNbq1yVKy4/YrUzoF+GdpaoZYGXvHTkpFaB+WhoTB6',
    'AWSALBCORS': 'osx6uR/c6KwcyMebiovRy3gAW+4aZLfcQPtU+6wJWwUnFm7qGQ3i1GXSVcIjBxrIJzLBkNrBn7CjRX6ixdUNbq1yVKy4/YrUzoF+GdpaoZYGXvHTkpFaB+WhoTB6',
    }
    headers = {
        'Accept': 'text/html, */*; q=0.01',
        'Accept-Language': 'ja-JP,ja;q=0.9,zh-TW;q=0.8,zh;q=0.7,en-US;q=0.6,en;q=0.5',
        'Connection': 'keep-alive',
        'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
        # 'Cookie': 'AWSALB=osx6uR/c6KwcyMebiovRy3gAW+4aZLfcQPtU+6wJWwUnFm7qGQ3i1GXSVcIjBxrIJzLBkNrBn7CjRX6ixdUNbq1yVKy4/YrUzoF+GdpaoZYGXvHTkpFaB+WhoTB6; AWSALBCORS=osx6uR/c6KwcyMebiovRy3gAW+4aZLfcQPtU+6wJWwUnFm7qGQ3i1GXSVcIjBxrIJzLBkNrBn7CjRX6ixdUNbq1yVKy4/YrUzoF+GdpaoZYGXvHTkpFaB+WhoTB6',
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


# %%
perfecture = get_sta_from_JMA(pd="00")
#Extract all perfecture id and name
#<div class="prefecture" id="pr33">岩手<input type="hidden" name="prid" value="33">
perfecture_list = re.findall(r'<div class="prefecture" id="pr(\d+)">(.+?)<input type="hidden" name="prid" value="\d+">', perfecture)

# %%
#Get station list for each perfecture
pool = []
for prid, prname in perfecture_list:
    #print(f"Downloading station list for {prname}...")
    sta_list_response = get_sta_from_JMA(pd=prid)
    # 使用正則表達式來找到所有的地區名稱和 ID
    pattern = r'<div style="width:100%; height:100%;" class="station"(.*?)<input type="hidden" name="kansoku"'
    matches = re.findall(pattern, sta_list_response, re.DOTALL)
    for m in matches:
        # title="地点名：好摩\nカナ:コウマ\n北緯：39度52.1分\n東経：141度10.0分\n標高：205m" onClick=""><input type="hidden" name="stid" value="a1032"><input type="hidden" name="stname" value="好摩"><input type="hidden" name="prid" value="33">
        stname = re.search(r'name="stname" value="(.*?)"', m).group(1)
        stid = re.search(r'name="stid" value="(.*?)"', m).group(1)
        lat = re.search(r'緯：(.*?)度(.*?)分', m).groups()
        #Convert 緯度(度) 緯度(分) 経度(度) 経度(分) to decimal
        lat = to_decimal(float(lat[0]), float(lat[1]))
        lon = re.search(r'経：(.*?)度(.*?)分', m).groups()
        lon = to_decimal(float(lon[0]), float(lon[1]))
        pool.append([stid, stname, lat, lon])


# %%
#Convert to pandas dataframe
JMA_STA_df = pd.DataFrame(pool, columns=["局ID", "局名", "緯度", "経度"])
#Combine JMA_STA_df and AMeDAS_STA_df based on 局名  and
JMA_STA_df["局名"] = JMA_STA_df["局名"].str.replace(" ", "")
AMeDAS_STA_df["局名"] = AMeDAS_STA_df["観測所名"].str.replace(" ", "")
merged = pd.merge(JMA_STA_df, AMeDAS_STA_df, on="局名", how="inner")


# %%
merged.to_csv(f"{ROOT}stations/merged_sta_list.csv", index=False)


# %%
#Drop duplicated 局ID
merged = pd.read_csv(f"{ROOT}stations/merged_sta_list.csv")
unique_sta_id = merged.drop_duplicates(subset="局ID")

# %%
def fetch_data_AMeDAS (station_id, year, month, session, sid):
    #Get the last day of the month
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
    response = session.post('https://www.data.jma.go.jp/risk/obsdl/show/table',data=data)
    #Set encoding
    response.encoding = response.apparent_encoding
    return response.text


# %%
import time
import gzip
#Parse the response as pandas dataframe
#fetch_data_AMeDAS("a1534", 2024, 2)
#Loop over all stations and from 2000-2024
#Save the data to a csv file in ROOT+"/weather_data/station_id/year-month.csv"

#Use session to visit homepage first
# 初始化 Session 物件
session = requests.Session()

# 設定 headers 和數據
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

# Conduct a GET request to the website to get the cookies
post_url = "https://www.data.jma.go.jp/risk/obsdl/index.php"
response = session.get(post_url, headers=headers)
#Set encoding
response.encoding = response.apparent_encoding
#Retrieve the sid 
#<input type="hidden" id="sid" value="oiug9r3jm0kn5if8se7p47b6o3" />
sid = re.search(r'<input type="hidden" id="sid" value="(.*?)"', response.text).group(1)

#Start fetching data
#Start time counter, stop after 5 hours
start_time = time.time()
for index, row in unique_sta_id.iterrows():
    station_id = row["局ID"]
    #Try creating the folder
    os.makedirs(f'{ROOT}/weather_data/{station_id}', exist_ok=True)
    for year in range(2000, 2025):
        for month in range(1, 13):
            path = f'{ROOT}/weather_data/{station_id}/{year}-{month}.csv.gz'
            #Check if the file already exists
            if os.path.exists(path):
                #Skip only "ダウンロードした時刻" in file
                #Open the file with utf8 encoding
                if "ダウンロードした時刻" in gzip.open(path, 'rt', encoding="utf8", errors='ignore').read():
                    print(f"Skipping data for {station_id} in {year}-{month}")
                    continue
            print(f"Downloading data for {station_id} in {year}-{month}")
            data = fetch_data_AMeDAS(station_id, year, month, session, sid)
            #Save the data to a csv file with utf8 encoding
            with gzip.open(path, 'wt', encoding="utf8") as f:
                f.write(data)

            #Sleep for 1 second
            time.sleep(2)
    #Stop after 5 hours
    if time.time() - start_time > 5 * 60 * 60:
        break



# %%
