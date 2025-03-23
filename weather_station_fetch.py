# %%
import requests
from bs4 import BeautifulSoup
import re
from urllib.parse import urljoin
import csv
import time

# 如果有安裝 jaconv 可轉換全形片假名為半形
try:
    import jaconv
    def to_halfwidth_katakana(s):
        return jaconv.z2h(s, kana=True, ascii=False, digit=False)
except ImportError:
    def to_halfwidth_katakana(s):
        return s

# 基礎網址設定
BASE_URL = "https://www.data.jma.go.jp/stats/etrn/select/"
DETAIL_BASE_URL = "https://www.data.jma.go.jp/stats/etrn/"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; Python script)"
}

def get_prefecture_mapping():
    """
    從對應頁面抓出各個 <area> 區域，
    取得 href 中的 prec_no 及 alt 屬性中的局名（去除「地方」）
    """
    url = urljoin(BASE_URL, "prefecture00.php?prec_no=11&block_no=47401&year=&month=&day=&view=")
    print("取得縣市對應資料：", url)
    resp = requests.get(url, headers=HEADERS)
    resp.encoding = resp.apparent_encoding
    soup = BeautifulSoup(resp.text, "html.parser")
    mapping = {}
    # 找出所有 href 包含 "prefecture.php?prec_no=" 的 <area> 標籤
    for area in soup.find_all("area", href=re.compile(r"prefecture\.php\?prec_no=")):
        href = area.get("href")
        m = re.search(r"prec_no=([\d]+)", href)
        if m:
            prec_no = m.group(1)
            # 取得 alt 內容，並移除結尾的「地方」
            region = area.get("alt", "").strip()
            if region.endswith("地方"):
                region = region[:-2]
            mapping[prec_no] = region
    return mapping

def parse_onmouseover(onmouseover_text):
    """
    解析 onmouseover 屬性中的 viewPoint(...) 呼叫，
    利用正規表達式取出參數（依照單引號分隔）
    預期參數順序：
      0: 類別代碼 (s 或 a)
      1: block_no
      2: 局名（漢字）
      3: 片假名名
      4: 緯度(度)
      5: 緯度(分)
      6: 経度(度)
      7: 経度(分)
      後續參數則不在本例需求中使用
    """
    args = re.findall(r"'(.*?)'", onmouseover_text)
    if len(args) < 8:
        return None
    data = {}
    data["type_code"] = args[0]
    data["block_no"] = args[1]
    data["station_name"] = args[2]
    data["katakana"] = args[3]
    try:
        lat_deg = float(args[4])
        lat_min = float(args[5])
        lon_deg = float(args[6])
        lon_min = float(args[7])
    except ValueError:
        return None
    # 將度分轉成十進位，並四捨五入到小數點後 4 位
    lat = round(lat_deg + lat_min/60, 4)
    lon = round(lon_deg + lon_min/60, 4)
    data["lat"] = lat
    data["lon"] = lon
    return data

def get_station_observation_number(detail_url):
    """
    從測站詳細頁面抓取觀測所編號，
    假設該頁面有一個表格，其中 th 標籤的文字包含「観測所番号」
    然後其相鄰的 td 中即為所需數值。
    """
    try:
        resp = requests.get(detail_url, headers=HEADERS)
        resp.encoding = resp.apparent_encoding
        soup = BeautifulSoup(resp.text, "html.parser")
        th = soup.find("th", string=re.compile("観測所番号"))
        if th:
            td = th.find_next_sibling("td")
            if td:
                obs_num_text = td.get_text(strip=True)
                obs_num_text = obs_num_text.replace(",", "")
                try:
                    obs_num = float(obs_num_text)
                    return obs_num
                except ValueError:
                    return obs_num_text
    except Exception as e:
        print("取得詳細頁面失敗：", detail_url, e)
    return ""

def get_station_list(prec_no, region):
    """
    針對單一縣市（prec_no），抓取該頁氣象站列表，
    並解析出每個站的資訊，包含後續需進一步抓取詳細頁面來取得觀測所編號。
    """
    url = urljoin(BASE_URL, f"prefecture.php?prec_no={prec_no}&block_no=&year=&month=&day=&view=")
    print("取得氣象站列表，prec_no =", prec_no, url)
    resp = requests.get(url, headers=HEADERS)
    resp.encoding = resp.apparent_encoding
    soup = BeautifulSoup(resp.text, "html.parser")
    station_data = []
    map_tag = soup.find("map", {"name": "point"})
    if not map_tag:
        print("未找到 map name='point'，prec_no", prec_no)
        return station_data
    for area in map_tag.find_all("area"):
        onmouseover = area.get("onmouseover", "")
        if not onmouseover:
            continue
        parsed = parse_onmouseover(onmouseover)
        if not parsed:
            continue
        # 局ID 為：類別代碼 + block_no（例如 s47401 或 a0002）
        station_id = f"{parsed['type_code']}{parsed['block_no']}"
        # 種類：若 type_code 為 s 則為「官」，若為 a 則為「四」
        station_type = "官" if parsed['type_code'] == "s" else "四"
        # 建構詳細頁面的 URL；原 href 為相對路徑，例如 "../index.php?prec_no=11&block_no=47401..."
        href = area.get("href", "")
        detail_url = urljoin(DETAIL_BASE_URL, href.replace("..", ""))
        # 從詳細頁面取得觀測所編號
        obs_num = get_station_observation_number(detail_url)
        # 轉換片假名（如果可能）為半形
        katakana = to_halfwidth_katakana(parsed["katakana"])
        row = {
            "局ID": station_id,
            "局名": parsed["station_name"],
            "緯度": parsed["lat"],
            "経度": parsed["lon"],
            "都府県振興局": region,
            "観測所番号": obs_num,
            "種類": station_type,
            "ｶﾀｶﾅ名": katakana,
            "prec_no": prec_no
        }
        station_data.append(row)
        # 為避免請求過快，稍作休息
        time.sleep(0.5)
    return station_data
# %%
def main():
    # 先取得各縣市對應資料
    mapping = get_prefecture_mapping()
    print("縣市對應資料：", mapping)
    all_stations = {}
    # 逐一以每個 prec_no 抓取氣象站列表
    for prec_no, region in mapping.items():
        stations = get_station_list(prec_no, region)
        for station in stations:
            station_id = station["局ID"]
            # 如果重複則略過（例如同一測站可能有多個 <area>）
            if station_id not in all_stations:
                all_stations[station_id] = station
    # 將結果寫入 CSV，欄位依需求順序排列
    fieldnames = ["局ID", "局名", "緯度", "経度", "都府県振興局", "観測所番号", "種類", "ｶﾀｶﾅ名", "prec_no"]
    with open("./stations/weather_stations.csv", "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for station in all_stations.values():
            writer.writerow(station)
    print("共儲存", len(all_stations), "筆氣象站資料，存檔：weather_stations.csv")

if __name__ == "__main__":
    main()

# %%
