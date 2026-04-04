import os
from datetime import datetime, timedelta

from amedas_obsdl_browser import (
    ObsdlBrowserClient,
    chunked,
    days_in_month,
    is_valid_month_file,
    load_station_records,
    logger,
    month_file_path,
    write_month_batch,
)


def main():
    now = datetime.now()
    batch_size = int(os.getenv('AMEDAS_BATCH_SIZE', '3'))
    time_limit_minutes = int(os.getenv('AMEDAS_TIME_LIMIT_MINUTES', '330'))
    stop_at = now + timedelta(minutes=time_limit_minutes)

    station_records = load_station_records()
    months = [(now.year, month) for month in range(1, now.month + 1)]

    client = ObsdlBrowserClient()
    try:
        for year, month in months:
            end_day = days_in_month(year, month)
            if (year, month) == (now.year, now.month):
                end_day = now.day - 1
            if end_day < 1:
                continue

            pending = [
                record
                for record in station_records
                if not is_valid_month_file(month_file_path(record.station_id, year, month), year, month)
                or (year, month) == (now.year, now.month)
            ]
            logger.info('full refresh target for %04d-%02d: %s stations', year, month, len(pending))
            for batch in chunked(pending, batch_size):
                if datetime.now() >= stop_at:
                    logger.info('time limit reached; stopping full refresh')
                    return
                write_month_batch(client, batch, year, month, end_day)
    finally:
        client.close()


if __name__ == '__main__':
    main()
