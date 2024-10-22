# %% Imports
import pandas as pd
import numpy as np
import os
import gzip
import re
from datetime import datetime, timedelta
from calendar import monthrange

# 定義根目錄
ROOT = "./"

# 定義氣象變數對應的列名稱
variables_mapping = {
    'Temperature': '気温(℃)',
    'Precipitation': '降水量(mm)',
    'Humidity': '相対湿度(％)',
}

# 定義需要的變數
variables = ['Temperature', 'Humidity', 'Precipitation']

# 定義氣候基準期間
climatology_start_year = 2000
climatology_end_year = 2020  # 包含 2020 年

# 定義資料儲存路徑
data_dir = f'{ROOT}/weather_data/'

# 載入站點資料
stations_df = pd.read_csv(f"{ROOT}stations/merged_sta_list.csv")

# 將站點列表去重複
unique_sta_id = stations_df.drop_duplicates(subset="局ID")

# 解析天氣資料的函數
def parse_weather_data(file_path):
    try:
        with gzip.open(file_path, 'rt', encoding="utf8", errors='ignore') as f:
            lines = f.readlines()
    except Exception as e:
        print(f"無法讀取檔案 {file_path}：{e}")
        return pd.DataFrame()

    # 刪除只有換行符的行
    lines = [line for line in lines if line.strip() != '']

    # 找到 '年月日時' 和 '品質情報' 的行索引
    d_line_idx, q_line_idx = -1, -1
    for i, line in enumerate(lines):
        if '年月日時' in line:
            d_line_idx = i
        if '品質情報' in line:
            q_line_idx = i
        if d_line_idx != -1 and q_line_idx != -1:
            break

    if d_line_idx == -1 or q_line_idx == -1:
        print(f"檔案 {file_path} 缺少必要的表頭資訊。")
        return pd.DataFrame()

    # 生成列名稱
    cols = []
    header_lines = lines[d_line_idx:q_line_idx+1]
    for col_values in zip(*[line.strip().split(',') for line in header_lines]):
        col_name = '_'.join([val for val in col_values if val])
        if "年月日時" in col_values:
            col_name = "年月日時"
        cols.append(col_name)

    # 讀取資料行
    data_lines = lines[q_line_idx+1:]
    if not data_lines:
        print(f"檔案 {file_path} 沒有資料。")
        return pd.DataFrame()

    data = [line.strip().split(',') for line in data_lines]
    df = pd.DataFrame(data, columns=cols)
    # 處理日期時間
    df['DateTime'] = pd.to_datetime(df['年月日時'], format='%Y/%m/%d %H:%M:%S', errors='coerce')
    df = df.dropna(subset=['DateTime'])
    df.set_index('DateTime', inplace=True)

    # 只保留需要的變數
    data = pd.DataFrame(index=df.index)
    for var_name, jp_name in variables_mapping.items():
        # 主資料列
        value_col = f"{jp_name}"
        # 品質情報列
        quality_col = f"{jp_name}_品質情報"
        if value_col in df.columns and quality_col in df.columns:
            data[var_name] = pd.to_numeric(df[value_col], errors='coerce')
            quality_info = pd.to_numeric(df[quality_col], errors='coerce')
            # 移除品質情報小於 8 的資料
            data[var_name] = data[var_name].where(quality_info >= 8, np.nan)
        else:
            data[var_name] = np.nan  # 沒有該變數的資料時，填充為 NaN

    return data

# 計算並儲存2000-2020年的氣候基準平均值
def calculate_and_save_climatology():
    print("正在計算2000-2020年氣候基準平均值...")
    for idx, row in unique_sta_id.iterrows():
        sta_id = str(row['局ID']).strip()
        station_name = row['局名']

        climatology_means_list = []

        # 計算每個站點2000-2020年的平均值
        for year in range(climatology_start_year, climatology_end_year + 1):
            climatology_monthly_data = []
            for month in range(1, 13):
                file_path = os.path.join(data_dir, sta_id, f"{year}-{month}.csv.gz")
                if os.path.exists(file_path):
                    df = parse_weather_data(file_path)
                    if df.empty:
                        continue
                    df['md'] = df.index.strftime('%m-%d')
                    climatology_monthly_data.append(df)

            if climatology_monthly_data:
                yearly_data = pd.concat(climatology_monthly_data)
                climatology_means_list.append(yearly_data)

        if climatology_means_list:
            # 計算2000-2020的氣候基準平均值
            climatology_data = pd.concat(climatology_means_list)
            climatology_means = climatology_data[variables].groupby(climatology_data.index.month).mean()

            # 將結果保存成CSV
            climatology_means.to_csv(f'./climatology/{sta_id}_climatology.csv')
            print(f"站點 {sta_id} 的氣候基準值已保存")
        else:
            print(f"站點 {sta_id} 在氣候基準期間沒有可用的資料。")


# %%
# 定義重建選項
rebuild = False

# 根據 rebuild 選項確定要計算的月份列表
today = datetime.now()
if rebuild:
    # 創建目錄並保存氣候基準值
    os.makedirs('./climatology', exist_ok=True)
    calculate_and_save_climatology()
    start_year, start_month = 2010, 1
    end_year, end_month = today.year, today.month
    target_months = [(year, month) for year in range(start_year, end_year + 1)
                     for month in range(1, 13) if (year > start_year or month >= start_month) and (year < end_year or month <= end_month)]
else:
    if today.day >= 1 and today.day <= 5:
        target_months = [
            (today.year, today.month),
            ((today - timedelta(days=1)).year, (today - timedelta(days=1)).month)
        ]
    else:
        target_months = [(today.year, today.month)]

# 計算距平值
for target_year, target_month in target_months:
    print(f"正在計算 {target_year} 年 {target_month} 月的距平值")

    if datetime(target_year, target_month, 1) < datetime(today.year, today.month, 1):
        period_start = datetime(target_year, target_month, 1)
        last_day = monthrange(target_year, target_month)[1]
        period_end = datetime(target_year, target_month, last_day, 23, 59, 59)
    else:
        period_start = datetime(target_year, target_month, 1)
        period_end = today

    current_md_list = [(period_start + timedelta(days=i)).strftime('%m-%d') for i in range((period_end - period_start).days + 1)]

    results = []

    for idx, row in unique_sta_id.iterrows():
        sta_id = str(row['局ID']).strip()
        station_name = row['局名']
        latitude = row['緯度']
        longitude = row['経度']

        # 讀取該站點的氣候基準值
        climatology_file = f'./climatology/{sta_id}_climatology.csv'
        if not os.path.exists(climatology_file):
            print(f"站點 {sta_id} 沒有保存的氣候基準值，跳過。")
            continue

        # 讀取基準值檔案
        climatology_data = pd.read_csv(climatology_file, index_col=0)
        climatology_means = climatology_data.loc[target_month, variables]

        # 收集當前年份的資料
        file_path = os.path.join(data_dir, sta_id, f"{target_year}-{target_month}.csv.gz")
        if os.path.exists(file_path):
            df = parse_weather_data(file_path)
            if df.empty:
                print(f"站點 {sta_id} 在 {target_year}-{target_month} 沒有可用的資料，跳過。")
                continue
            df_period = df[(df.index >= period_start) & (df.index <= period_end)]
            if df_period.empty:
                print(f"站點 {sta_id} 在期間內沒有資料，跳過。")
                continue
            current_data = df_period
        else:
            print(f"站點 {sta_id} 在 {target_year}-{target_month} 沒有資料檔案，跳過。")
            continue

        # 計算當前年份的平均值
        current_means = current_data[variables].mean()

        if current_means.isnull().any() or climatology_means.isnull().any():
            print(f"站點 {sta_id} 資料缺失，跳過。")
            continue

        anomalies = current_means - climatology_means

        result = {
            '站名': station_name,
            '緯度': latitude,
            '經度': longitude,
            '平均溫距平': anomalies['Temperature'],
            '本年度平均溫': current_means['Temperature'],
            '平均溼度距平': anomalies['Humidity'],
            '本年度日均相對溼度': current_means['Humidity'],
            '本年度日均降水量': current_means['Precipitation'],
            '日均降水量距平': anomalies['Precipitation'],
            '站號': sta_id
        }

        results.append(result)

    # 儲存結果
    results_df = pd.DataFrame(results)
    if not results_df.empty:
        round_columns = ['平均溫距平', '本年度平均溫', '平均溼度距平', '本年度日均相對溼度', '本年度日均降水量', '日均降水量距平']
        results_df[round_columns] = results_df[round_columns].round(2)
        results_df['緯度'] = results_df['緯度'].round(6)
        results_df['經度'] = results_df['經度'].round(6)

        csv_filename = f'./anomaly/result/{target_year}_{target_month:02d}.csv'
        json_filename = f'./anomaly/result/{target_year}_{target_month:02d}.json'

        results_df.to_csv(csv_filename, index=False, encoding='utf-8-sig')
        results_df.to_json(json_filename, orient='records', force_ascii=False)

        print(f"結果已保存到 {csv_filename} 和 {json_filename}")
    else:
        print(f"在 {target_year} 年 {target_month} 月沒有可用的資料。")

# %%
