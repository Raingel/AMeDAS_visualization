import gzip
import html
import json
import logging
import os
import re
import shutil
import time
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Sequence

import pandas as pd
from selenium import webdriver
from selenium.webdriver.chrome.options import Options as ChromeOptions
from selenium.webdriver.edge.options import Options as EdgeOptions


ROOT = Path('.')
OBS_DL_URL = 'https://www.data.jma.go.jp/risk/obsdl/'
SHOW_TABLE_PATH = '/risk/obsdl/show/table'
TIMESTAMP_PREFIX = 'ダウンロードした時刻：'
ELEMENT_NUM_LIST = [
    ['201', ''],
    ['101', ''],
    ['503', ''],
    ['401', ''],
    ['501', ''],
    ['301', ''],
    ['612', ''],
    ['604', ''],
    ['605', ''],
    ['602', ''],
    ['601', ''],
    ['610', ''],
    ['703', ''],
    ['607', ''],
    ['704', ''],
]
QUALITY_EIGHT_TITLES = {
    '気温(℃)',
    '降水量(mm)',
    '降雪(cm)',
    '日照時間(時間)',
    '積雪(cm)',
    '風速(m/s)',
    '風向',
    '露点温度(℃)',
    '蒸気圧(hPa)',
    '相対湿度(％)',
}
FIELD_RE = re.compile(r'^data(?P<station>\d+)_(?P<element>\d+)_(?P<suffix>0|wind)$')
PERIOD_RE = re.compile(r'^(?P<year>\d+)年(?P<month>\d+)月(?P<day>\d+)日(?P<hour>\d+)時$')


@dataclass(frozen=True)
class StationRecord:
    station_id: str
    station_name: str


@dataclass(frozen=True)
class ColumnSpec:
    title: str
    value_field: str
    direction_field: Optional[str] = None

    @property
    def is_wind_pair(self) -> bool:
        return self.direction_field is not None


def get_logger() -> logging.Logger:
    log_level = os.getenv('AMEDAS_LOG_LEVEL', 'INFO').upper()
    logging.basicConfig(
        level=getattr(logging, log_level, logging.INFO),
        format='%(asctime)s %(levelname)s: %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
    )
    return logging.getLogger(__name__)


logger = get_logger()


def load_station_sampling_config():
    sample_only = os.getenv('AMEDAS_SAMPLE_ONLY', '0') == '1'
    sample_stations = [s.strip() for s in os.getenv('AMEDAS_SAMPLE_STATIONS', '').split(',') if s.strip()]
    sample_limit = int(os.getenv('AMEDAS_SAMPLE_LIMIT', '5'))
    return sample_only, sample_stations, sample_limit


def load_station_records(root: Path = ROOT) -> List[StationRecord]:
    stations_path = root / 'stations' / 'weather_stations.csv'
    df = pd.read_csv(stations_path)
    id_col = df.columns[0]
    name_col = df.columns[1]
    deduped = df.drop_duplicates(subset=id_col)
    records = [
        StationRecord(str(row[id_col]).strip(), str(row[name_col]).strip())
        for _, row in deduped.iterrows()
    ]
    sample_only, sample_stations, sample_limit = load_station_sampling_config()
    if not sample_only:
        return records
    if sample_stations:
        wanted = set(sample_stations)
        records = [record for record in records if record.station_id in wanted]
    if sample_limit > 0:
        records = records[:sample_limit]
    logger.warning('sample mode active: %s stations -> %s', len(records), [record.station_id for record in records])
    return records


def iter_months(start_year: int, start_month: int, end_year: int, end_month: int):
    year, month = start_year, start_month
    while (year, month) <= (end_year, end_month):
        yield year, month
        month += 1
        if month > 12:
            year += 1
            month = 1


def previous_month(year: int, month: int):
    if month == 1:
        return year - 1, 12
    return year, month - 1


def days_in_month(year: int, month: int) -> int:
    next_month = pd.Timestamp(year, month, 1) + pd.DateOffset(months=1)
    return int((next_month - pd.DateOffset(days=1)).day)


def month_file_path(station_id: str, year: int, month: int, root: Path = ROOT) -> Path:
    return root / 'weather_data' / station_id / f'{year}-{month}.csv.gz'


def chunked(items: Sequence[StationRecord], size: int):
    for idx in range(0, len(items), size):
        yield items[idx: idx + size]


def get_download_timestamp(path: Path) -> Optional[datetime]:
    if not path.exists():
        return None
    try:
        with gzip.open(path, 'rt', encoding='cp932', errors='ignore') as fh:
            first_line = fh.readline().strip()
    except OSError:
        return None
    if not first_line.startswith(TIMESTAMP_PREFIX):
        return None
    try:
        return datetime.strptime(first_line[len(TIMESTAMP_PREFIX):], '%Y/%m/%d %H:%M:%S')
    except ValueError:
        return None


def get_first_data_year_month(path: Path) -> Optional[tuple]:
    if not path.exists():
        return None
    try:
        with gzip.open(path, 'rt', encoding='cp932', errors='ignore') as fh:
            for _ in range(6):
                next(fh, '')
            first_data = next(fh, '').strip()
    except OSError:
        return None
    if not first_data:
        return None
    timestamp = first_data.split(',', 1)[0]
    match = re.match(r'^(?P<year>\d{4})/(?P<month>\d{1,2})/', timestamp)
    if not match:
        return None
    return int(match.group('year')), int(match.group('month'))


def is_valid_month_file(path: Path, year: int, month: int) -> bool:
    if not path.exists():
        return False
    if get_download_timestamp(path) is None:
        return False
    data_month = get_first_data_year_month(path)
    if data_month is None:
        return True
    return data_month == (year, month)


def should_refresh_recent_file(path: Path, year: int, month: int, max_age_hours: int = 24) -> bool:
    if not is_valid_month_file(path, year, month):
        return True
    downloaded_at = get_download_timestamp(path)
    if downloaded_at is None:
        return True
    return datetime.now() - downloaded_at >= timedelta(hours=max_age_hours)


def dump_http_trace(station_ids: Sequence[str], year: int, month: int, payload: Dict[str, str], text: str):
    if os.getenv('AMEDAS_SAVE_HTTP_DUMP', '0') != '1':
        return
    dump_dir = Path(os.getenv('AMEDAS_HTTP_DUMP_DIR', 'debug_http'))
    dump_dir.mkdir(parents=True, exist_ok=True)
    for station_id in station_ids:
        base = f'{station_id}_{year}_{month}'
        payload_path = dump_dir / f'{base}_payload.txt'
        response_path = dump_dir / f'{base}_response_head.txt'
        with open(payload_path, 'w', encoding='utf-8') as fh:
            for key, value in payload.items():
                fh.write(f'{key}={value}\n')
        with open(response_path, 'w', encoding='utf-8') as fh:
            fh.write(text[:4000])


def html_text_segments(value: str) -> List[str]:
    segments = re.findall(r'>([^<]+)<', value)
    return [html.unescape(segment).strip() for segment in segments if segment.strip()]


def build_column_specs(header: List[dict], station_ids: Sequence[str]) -> Dict[str, List[ColumnSpec]]:
    grouped: Dict[int, Dict[int, dict]] = {}
    for column in header:
        field = column.get('field', '')
        if field == 'period':
            continue
        match = FIELD_RE.match(field)
        if not match:
            continue
        station_idx = int(match.group('station'))
        element_idx = int(match.group('element'))
        suffix = match.group('suffix')
        texts = html_text_segments(column.get('name', ''))
        title = texts[-1] if texts else field
        grouped.setdefault(station_idx, {}).setdefault(element_idx, {})
        grouped[station_idx][element_idx][suffix] = field
        if suffix == '0':
            grouped[station_idx][element_idx]['title'] = title
    specs_by_station: Dict[str, List[ColumnSpec]] = {}
    for station_idx, station_id in enumerate(station_ids):
        per_station = grouped.get(station_idx, {})
        specs: List[ColumnSpec] = []
        for _, entry in sorted(per_station.items()):
            if '0' not in entry:
                continue
            specs.append(
                ColumnSpec(
                    title=entry.get('title', entry['0']),
                    value_field=entry['0'],
                    direction_field=entry.get('wind'),
                )
            )
        specs_by_station[station_id] = specs
    return specs_by_station


def parse_period_timestamp(period: str) -> str:
    match = PERIOD_RE.match(period.strip())
    if not match:
        raise ValueError(f'unexpected period format: {period!r}')
    year = int(match.group('year'))
    month = int(match.group('month'))
    day = int(match.group('day'))
    hour = int(match.group('hour'))
    return f'{year}/{month}/{day} {hour}:00:00'


def default_quality_code(title: str) -> str:
    return '8' if title in QUALITY_EIGHT_TITLES else '0'


def build_station_csv_text(station_name: str, specs: List[ColumnSpec], rows: List[dict], fetched_at: datetime) -> str:
    line_station = ['']
    line_header = ['年月日時']
    line_extra1 = ['']
    line_extra2 = ['']
    for spec in specs:
        if spec.is_wind_pair:
            line_station.extend([station_name] * 5)
            line_header.extend([spec.title] * 5)
            line_extra1.extend(['', '', '風向', '風向', ''])
            line_extra2.extend(['', '品質情報', '', '品質情報', '均質番号'])
        else:
            line_station.extend([station_name] * 3)
            line_header.extend([spec.title] * 3)
            line_extra1.extend(['', '', ''])
            line_extra2.extend(['', '品質情報', '均質番号'])
    csv_lines = [
        f'{TIMESTAMP_PREFIX}{fetched_at:%Y/%m/%d %H:%M:%S}',
        '',
        ','.join(line_station),
        ','.join(line_header),
        ','.join(line_extra1),
        ','.join(line_extra2),
    ]
    for row in rows:
        values = [parse_period_timestamp(row['period'])]
        for spec in specs:
            if spec.is_wind_pair:
                values.extend([
                    str(row.get(spec.value_field, '')),
                    default_quality_code(spec.title),
                    str(row.get(spec.direction_field or '', '')),
                    default_quality_code('風向'),
                    '1',
                ])
            else:
                values.extend([
                    str(row.get(spec.value_field, '')),
                    default_quality_code(spec.title),
                    '1',
                ])
        csv_lines.append(','.join(values))
    return '\n'.join(csv_lines)


def write_station_month_file(path: Path, text: str):
    path.parent.mkdir(parents=True, exist_ok=True)
    with gzip.open(path, 'wt', encoding='cp932') as fh:
        fh.write(text)


def detect_browser():
    preferred = os.getenv('AMEDAS_BROWSER', 'auto').lower()
    forced_binary = os.getenv('AMEDAS_BROWSER_BIN')
    candidates = []
    if preferred in ('auto', 'chrome'):
        chrome_bins = [
            forced_binary if preferred == 'chrome' and forced_binary else None,
            shutil.which('google-chrome'),
            shutil.which('google-chrome-stable'),
            shutil.which('chromium'),
            shutil.which('chromium-browser'),
            r'C:\Program Files\Google\Chrome\Application\chrome.exe',
        ]
        candidates.append(('chrome', next((path for path in chrome_bins if path and Path(path).exists()), None)))
    if preferred in ('auto', 'edge'):
        edge_bins = [
            forced_binary if preferred == 'edge' and forced_binary else None,
            shutil.which('microsoft-edge'),
            shutil.which('microsoft-edge-stable'),
            shutil.which('msedge'),
            r'C:\Program Files (x86)\Microsoft\Edge\Application\msedge.exe',
        ]
        candidates.append(('edge', next((path for path in edge_bins if path and Path(path).exists()), None)))
    for browser_name, binary in candidates:
        if binary:
            return browser_name, binary
    raise RuntimeError('could not find Chrome or Edge; set AMEDAS_BROWSER_BIN')


class ObsdlBrowserClient:
    def __init__(self):
        self.recycle_after = int(os.getenv('AMEDAS_BROWSER_RECYCLE_AFTER', '200'))
        self._request_count = 0
        self.driver = None
        self.browser_name = None
        self.binary = None
        self._start_driver()

    def _start_driver(self):
        self.browser_name, self.binary = detect_browser()
        logger.info('starting %s browser: %s', self.browser_name, self.binary)
        if self.browser_name == 'chrome':
            options = ChromeOptions()
            options.binary_location = self.binary
            options.add_argument('--headless=new')
            options.add_argument('--disable-gpu')
            options.add_argument('--window-size=1600,1200')
            options.add_argument('--no-sandbox')
            options.add_argument('--disable-dev-shm-usage')
            self.driver = webdriver.Chrome(options=options)
        else:
            options = EdgeOptions()
            options.binary_location = self.binary
            options.add_argument('--headless=new')
            options.add_argument('--disable-gpu')
            options.add_argument('--window-size=1600,1200')
            options.add_argument('--no-sandbox')
            options.add_argument('--disable-dev-shm-usage')
            self.driver = webdriver.Edge(options=options)
        self.driver.set_page_load_timeout(int(os.getenv('AMEDAS_BROWSER_PAGE_TIMEOUT', '120')))
        self.driver.set_script_timeout(int(os.getenv('AMEDAS_BROWSER_SCRIPT_TIMEOUT', '180')))
        self.driver.get(OBS_DL_URL)
        self._request_count = 0

    def close(self):
        if self.driver is not None:
            self.driver.quit()
            self.driver = None

    def recycle_if_needed(self):
        if self._request_count < self.recycle_after:
            return
        logger.info('recycling browser session after %s requests', self._request_count)
        self.close()
        self._start_driver()

    def fetch_month_json(self, station_ids: Sequence[str], year: int, month: int, end_day: int):
        request_delay = float(os.getenv('AMEDAS_REQUEST_DELAY_SECONDS', '1.0'))
        payload = {
            'stationNumList': json.dumps(list(station_ids), ensure_ascii=False),
            'aggrgPeriod': '9',
            'elementNumList': json.dumps(ELEMENT_NUM_LIST, ensure_ascii=False),
            'interAnnualType': '1',
            'ymdList': json.dumps([str(year), str(year), str(month), str(month), '1', str(end_day)], ensure_ascii=False),
            'optionNumList': '[]',
            'downloadFlag': 'false',
            'selectedPageNum': '1',
            'rmkFlag': '1',
            'disconnectFlag': '1',
            'kijiFlag': '0',
            'youbiFlag': '0',
            'fukenFlag': '0',
            'jikantaiFlag': '0',
            'jikantaiList': '[1,24]',
        }
        script = """
const payload = arguments[0];
const path = arguments[1];
const done = arguments[arguments.length - 1];
(async () => {
  try {
    const body = new URLSearchParams(payload);
    const res = await fetch(path, {
      method: 'POST',
      headers: {'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8'},
      body,
      credentials: 'include'
    });
    done({status: res.status, text: await res.text()});
  } catch (err) {
    done({status: 0, text: String(err)});
  }
})();
"""
        for attempt in range(1, 4):
            self.recycle_if_needed()
            result = self.driver.execute_async_script(script, payload, SHOW_TABLE_PATH)
            self._request_count += 1
            text = result.get('text', '')
            status = int(result.get('status', 0))
            dump_http_trace(station_ids, year, month, payload, text)
            if status == 200 and text.lstrip().startswith('{'):
                if request_delay > 0:
                    time.sleep(request_delay)
                return json.loads(text)
            logger.warning(
                'unexpected JMA response for %s %04d-%02d on attempt %s: status=%s prefix=%r',
                list(station_ids),
                year,
                month,
                attempt,
                status,
                text[:200],
            )
            if status == 429:
                backoff = min(30, 5 * attempt)
                logger.info('rate limited by JMA; sleeping %s seconds before retry', backoff)
                time.sleep(backoff)
            self.driver.get(OBS_DL_URL)
        raise RuntimeError(f'failed to fetch JMA data for {station_ids} {year}-{month:02d}')


def write_month_batch(
    client: ObsdlBrowserClient,
    station_batch: Sequence[StationRecord],
    year: int,
    month: int,
    end_day: int,
    root: Path = ROOT,
):
    response = client.fetch_month_json([record.station_id for record in station_batch], year, month, end_day)
    specs_by_station = build_column_specs(response['header'], [record.station_id for record in station_batch])
    fetched_at = datetime.now()
    for record in station_batch:
        specs = specs_by_station.get(record.station_id, [])
        if not specs:
            logger.warning('[%s] no columns returned for %04d-%02d', record.station_id, year, month)
            continue
        text = build_station_csv_text(record.station_name, specs, response['data'], fetched_at)
        write_station_month_file(month_file_path(record.station_id, year, month, root=root), text)
        logger.info('[%s] wrote %04d-%02d', record.station_id, year, month)
