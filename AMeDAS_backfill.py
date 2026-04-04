import os
from datetime import datetime, timedelta

from amedas_obsdl_browser import (
    ObsdlBrowserClient,
    chunked,
    days_in_month,
    iter_months,
    is_valid_month_file,
    load_station_records,
    logger,
    month_file_path,
    write_month_batch,
)


def main():
    now = datetime.now()
    start_ym = os.getenv('AMEDAS_BACKFILL_START', '2025-10')
    start_year, start_month = map(int, start_ym.split('-'))
    batch_size = int(os.getenv('AMEDAS_BATCH_SIZE', '3'))
    time_limit_minutes = int(os.getenv('AMEDAS_TIME_LIMIT_MINUTES', '330'))
    stop_at = now + timedelta(minutes=time_limit_minutes)

    station_records = load_station_records()
    client = ObsdlBrowserClient()
    try:
        for year, month in iter_months(start_year, start_month, now.year, now.month):
            end_day = days_in_month(year, month)
            if (year, month) == (now.year, now.month):
                end_day = now.day - 1
            if end_day < 1:
                logger.info('skip %04d-%02d because there is no completed day yet', year, month)
                continue

            pending = [
                record
                for record in station_records
                if not is_valid_month_file(month_file_path(record.station_id, year, month), year, month)
            ]
            logger.info('backfill target for %04d-%02d: %s stations', year, month, len(pending))
            for batch in chunked(pending, batch_size):
                if datetime.now() >= stop_at:
                    logger.info('time limit reached; stopping backfill')
                    return
                write_month_batch(client, batch, year, month, end_day)
    finally:
        client.close()


if __name__ == '__main__':
    main()
