# %%
import requests
import zipfile
import pandas as pd
import os
import re
import time
import gzip
from datetime import datetime, timedelta

# 轉換度、分為十進位格式
def to_decimal(d, m):
    return d + m / 60

ROOT = "./"

# 嘗試用多種編碼讀取 CSV
def read_csv_with_multiple_encodings(file_path, encodings=['cp932', 'utf-8', 'shift_jis', 'euc-jp']):
    for encoding in encodings:
        try:
            df = pd.read_csv(file_path, encoding=encoding)
            print(f"Successfully read the file with encoding: {encoding}")
            return df
        except UnicodeDecodeError as e:
            print(f"Failed to read with encoding {encoding}: {e}")
    raise ValueError("Failed to read the file with all provided encodings")

# 下載並解壓 AMeDAS 站點清單，並轉換緯度、經度
def download_amedas_station_list():
    AMeDAS_STA_list = "https://www.jma.go.jp/jma/kishou/know/amedas/ame_master.zip"
    r = requests.get(AMeDAS_STA_list)
    with open("ame_master.zip", "wb") as code:
        code.write(r.content)
    with zipfile.ZipFile("ame_master.zip", "r") as z:
        z.extractall("ame_master")

    # 找出解壓後唯一的 CSV 檔案
    for file in os.listdir("ame_master"):
        if file.endswith(".csv"):
            AMeDAS_STA_file = file
            AMeDAS_STA_df = read_csv_with_multiple_encodings("ame_master/" + AMeDAS_STA_file)
            break

    AMeDAS_STA_df["緯度"] = AMeDAS_STA_df.apply(lambda x: to_decimal(x["緯度(度)"], x["緯度(分)"]), axis=1)
    AMeDAS_STA_df["経度"] = AMeDAS_STA_df.apply(lambda x: to_decimal(x["経度(度)"], x["経度(分)"]), axis=1)
    AMeDAS_STA_df.to_csv("ame_master/" + AMeDAS_STA_file, index=False)
    return AMeDAS_STA_df

# 舊版取得局點資料的函式（保留不變）
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

# 主要的下載資料函式
def download_weather_data(unique_sta_id, time_limit_minutes=10):
    start_time = time.time()
    max_time_seconds = time_limit_minutes * 60
    time_exceeded = False

    download_time_str = "ダウンロードした時刻：" + datetime.now().strftime("%Y/%m/%d %H:%M:%S")
    header_columns = "年月日時,気温(℃),気温(℃),気温(℃),降水量(mm),降水量(mm),降水量(mm),日射量(MJ/㎡),日射量(MJ/㎡),日射量(MJ/㎡),天気,天気,天気,視程(km),視程(km),視程(km),雲量(10分比),雲量(10分比),雲量(10分比),現地気圧(hPa),現地気圧(hPa),現地気圧(hPa),海面気圧(hPa),海面気圧(hPa),海面気圧(hPa),相対湿度(％),相対湿度(％),相対湿度(％),風速(m/s),風速(m/s),風速(m/s),風速(m/s),風速(m/s),日照時間(時間),日照時間(時間),日照時間(時間),積雪(cm),積雪(cm),積雪(cm),降雪(cm),降雪(cm),降雪(cm)"
    header_extra1 = ",,,,,,,,,,,,,,,,,,,,,,,,,,,風向,風向,,,,,,,,,"
    header_extra2 = ",,品質情報,均質番号,,品質情報,均質號碼,,品質情報,均質號碼,,品質情報,均質號碼,,品質情報,均質號碼,,品質情報,均質號碼,,品質情報,均質號碼,,品質情報,均質號碼,,品質情報,均質號碼,,品質情報,,品質情報,均質號碼,,品質情報,均質號碼,,品質情報,均質號碼,,品質情報,均質號碼"
    
    current_date = datetime.now()
    current_year = current_date.year
    current_month = current_date.month
    previous_month = current_month - 1 if current_month > 1 else 12
    previous_year = current_year if current_month > 1 else current_year - 1
    # 若今天尚未滿 13 號，則只下載上個月的資料；否則下載當月資料
    months_to_download = []
    if current_date.day < 13:
        months_to_download.append((previous_year, previous_month))
    else:
        months_to_download.append((current_year, current_month))
        # 如有需要，也可考慮同時下載上個月
        # months_to_download.append((previous_year, previous_month))
    
    # 定義 prec_no 的對應字典
    prec_mapping = {
        "三重": "53",
        "上川": "12",
        "京都": "61",
        "佐賀": "85",
        "兵庫": "63",
        "十勝": "20",
        "千葉": "45",
        "和歌": "65",  # 和歌山取前兩字「和歌」
        "埼玉": "43",
        "大分": "83",
        "大阪": "62",
        "奈良": "64",
        "宗谷": "11",
        "宮城": "34",
        "宮崎": "87",
        "富山": "55",
        "山口": "81",
        "山形": "35",
        "山梨": "49",
        "岐阜": "52",
        "岡山": "66",
        "岩手": "33",
        "島根": "68",
        "広島": "67",
        "後志": "16",
        "徳島": "71",
        "愛媛": "73",
        "愛知": "51",
        "新潟": "54",
        "日高": "22",
        "東京": "44",
        "栃木": "41",
        "根室": "18",
        "檜山": "24",
        "沖縄": "91",
        "渡島": "23",
        "滋賀": "60",
        "熊本": "86",
        "留萌": "13",
        "石川": "56",
        "石狩": "14",
        "神奈": "46",  # 神奈川取前兩字
        "福井": "57",
        "福岡": "82",
        "福島": "36",
        "秋田": "32",
        "空知": "15",
        "群馬": "42",
        "胆振": "21",
        "ｵﾎ": "17",
        "釧路": "19",
        "青森": "31",
        "茨城": "40",
        "長野": "48",
        "静岡": "50",
        "鳥取": "69",
        "香川": "72",
        "高知": "74",
        "長崎": "84",
        "鹿児島": "88",
        "南極": "99"
    }
    
    for idx, row in unique_sta_id.iterrows():
        # 檢查是否超過時間限制（站點層級）
        if time.time() - start_time > max_time_seconds:
            print("已超過設定的執行時間限制，終止後續站點的處理。")
            return

        station_id = row["局ID"]
        station_name = row["局名"]
        block_no = str(row.get("局ID", "00000"))[1:]
        sta_type = str(row.get("局ID", "")).strip()[0]
        key = str(row.get("都府県振興局", "")).strip()[:2]
        prec_no = prec_mapping.get(key, "33")
        os.makedirs(f'{ROOT}/weather_data/{station_id}', exist_ok=True)
        
        for (year, month) in months_to_download:
            # 檢查是否超過時間限制（月份層級）
            if time.time() - start_time > max_time_seconds:
                print("已超過設定的執行時間限制，終止後續月份的處理。")
                time_exceeded = True
                break

            start_date = datetime(year, month, 1)
            if year == current_year and month == current_month:
                next_month = start_date.replace(day=28) + timedelta(days=4)
                last_day = (next_month - timedelta(days=next_month.day)).day
                max_day = min(last_day, current_date.day - 1)
            else:
                next_month = start_date.replace(day=28) + timedelta(days=4)
                max_day = (next_month - timedelta(days=next_month.day)).day
            
            csv_path = f'{ROOT}/weather_data/{station_id}/{year}-{month}.csv.gz'
            if os.path.exists(csv_path):
                try:
                    with gzip.open(csv_path, 'rt', encoding='utf8') as f:
                        first_line = f.readline().strip()
                    if first_line.startswith("ダウンロードした時刻："):
                        prev_dt = datetime.strptime(first_line[len("ダウンロードした時刻："):], "%Y/%m/%d %H:%M:%S")
                        # 資料若在過去 24 小時內更新則跳過
                        if (datetime.now() - prev_dt).total_seconds() < 0 * 3600:
                            print(f"站點 {station_id} {year}-{month} 的資料在過去 24 小時內已更新，跳過下載。")
                            continue
                except Exception as e:
                    print(f"檢查站點 {station_id} {year}-{month} 的現有 CSV 時出現錯誤：{e}")
            
            output_rows = []
            for day in range(1, max_day + 1):
                # 檢查是否超過時間限制（每日層級）
                if time.time() - start_time > max_time_seconds:
                    print("已超過設定的執行時間限制，終止後續日資料的處理。")
                    time_exceeded = True
                    break

                url = f"https://www.data.jma.go.jp/stats/etrn/view/hourly_{sta_type}1.php?prec_no={prec_no}&block_no={block_no}&year={year}&month={month:02d}&day={day:02d}&view="
                print(f"下載站點 {station_id} {year}-{month}-{day} 的資料，使用 prec_no {prec_no}，url: {url}")
                try:
                    df_day = pd.read_html(url)[0]
                    
                    # 扁平化多層索引的欄位名稱
                    new_columns = []
                    for col in df_day.columns:
                        if isinstance(col, tuple):
                            # 若 tuple 有第二個值且不為空，則取第二個值，否則取第一個
                            if len(col) > 1 and col[1] and pd.notnull(col[1]):
                                colname = col[1]
                            else:
                                colname = col[0]
                        else:
                            colname = col
                        new_columns.append(str(colname).replace("\n", "").strip())
                    df_day.columns = new_columns
                    
                    # 判斷是否為新格式（檢查是否包含「風速・風向」字樣）
                    if any("平均風速" in col for col in df_day.columns):
                        # 新格式的欄位對應設定
                        rename_map = {
                            "時": "hour",
                            "降水量 (mm)": "precipitation",
                            "気温 (℃)": "temperature",
                            "湿度 (％)": "humidity",
                            "平均風速 (m/s)": "wind_speed",
                            "風向": "wind_direction",
                            "日照 時間 (h)": "sunshine",
                            "降雪 (cm)": "snowfall",
                            "積雪 (cm)": "snow_depth"
                        }
                    else:
                        # 舊格式的欄位對應設定
                        rename_map = {
                            "時": "hour",
                            "現地": "pressure_local",
                            "海面": "pressure_sea",
                            "降水量 (mm)": "precipitation",
                            "気温 (℃)": "temperature",
                            "湿度 (％)": "humidity",
                            "風速": "wind_speed",
                            "風向": "wind_direction",
                            "日照 時間 (h)": "sunshine",
                            "全天 日射量 (MJ/㎡)": "solar",
                            "雪": "snow_depth",  # 注意舊格式中「雪」可能包含降雪與積雪，需要依狀況處理
                            "天気": "weather",
                            "雲量": "cloud",
                            "視程 (km)": "visibility"
                        }
                    df_day.rename(columns=rename_map, inplace=True)
                    print(df_day)
                    # 逐行處理並組出 42 欄資料（每個觀測值後接品質資訊與均質號碼）
                    for i, r in df_day.iterrows():
                        try:
                            hour_val = int(r["hour"])
                        except Exception:
                            continue
                        timestamp = f"{year}/{month}/{day} {hour_val}:00:00"
                        
                        temperature    = r.get("temperature", "")
                        precipitation  = r.get("precipitation", "")
                        solar          = r.get("solar", "")
                        weather        = r.get("weather", "")
                        visibility     = r.get("visibility", "")
                        cloud          = r.get("cloud", "")
                        pressure_local = r.get("pressure_local", "")
                        pressure_sea   = r.get("pressure_sea", "")
                        humidity       = r.get("humidity", "")
                        wind_speed     = r.get("wind_speed", "")
                        wind_direction = r.get("wind_direction", "")
                        sunshine       = r.get("sunshine", "")
                        snow_depth     = r.get("snow_depth", "")
                        snowfall       = r.get("snowfall", "")
                        
                        row_out = [
                            timestamp,
                            temperature, "8", "1",
                            precipitation, "8", "1",
                            solar, "0", "1",
                            weather, "0", "1",
                            visibility, "0", "1",
                            cloud, "0", "1",
                            pressure_local, "0", "1",
                            pressure_sea, "0", "1",
                            humidity, "0", "1",
                            wind_speed, "8", wind_direction, "8", "1",
                            sunshine, "0", "1",
                            snow_depth, "0", "1",
                            snowfall, "0", "1"
                        ]
                        if len(row_out) != 42:
                            print("Row length mismatch:", len(row_out))
                        output_rows.append(row_out)
                    print(output_rows)
                    time.sleep(1)
                except Exception as e:
                    print(f"處理站點 {station_id} {year}-{month}-{day} 時發生錯誤：{e}")
                    continue
            
            # 若有資料則輸出為 CSV（gzip 壓縮），並確保與舊版格式相符
            if output_rows:
                csv_lines = []
                csv_lines.append(download_time_str)
                csv_lines.append("")
                csv_lines.append("," + ",".join([station_name] * 42))
                csv_lines.append(header_columns)
                csv_lines.append(header_extra1)
                csv_lines.append(header_extra2)
                for row_vals in output_rows:
                    row_str = ",".join(str(v) for v in row_vals)
                    csv_lines.append(row_str)
                csv_content = "\n".join(csv_lines)
                with gzip.open(csv_path, 'wt', encoding="utf8") as f:
                    f.write(csv_content)
                print(f"成功寫入站點 {station_id} {year}-{month} 的資料。")
            else:
                print(f"未找到站點 {station_id} {year}-{month} 的資料。")
            
            if time_exceeded:
                print("因執行時間超過設定，停止下載後續月份。")
                break
        if time_exceeded:
            print("因執行時間超過設定，停止下載後續站點。")
            break

# %%
# 讀取合併後的站點清單，對局ID 去除重複後下載資料
combined_df = pd.read_csv(f"{ROOT}stations/merged_sta_list.csv")
unique_sta_id = combined_df.drop_duplicates(subset="局ID")
#先只做a0002
#unique_sta_id = unique_sta_id[unique_sta_id["局ID"] == "a0002"]
download_weather_data(unique_sta_id, time_limit_minutes=300)

# %%
