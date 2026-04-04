import os
from datetime import datetime, timedelta

from amedas_obsdl_browser import (
    ObsdlBrowserClient,
    chunked,
    days_in_month,
    load_station_records,
    logger,
    month_file_path,
    previous_month,
    should_refresh_recent_file,
    write_month_batch,
)


def main():
    now = datetime.now()
    max_age_hours = int(os.getenv('AMEDAS_RECENT_MAX_AGE_HOURS', '24'))
    batch_size = int(os.getenv('AMEDAS_BATCH_SIZE', '3'))
    time_limit_minutes = int(os.getenv('AMEDAS_TIME_LIMIT_MINUTES', '330'))
    stop_at = now + timedelta(minutes=time_limit_minutes)

    months = []
    if now.day <= 11:
        months.append(previous_month(now.year, now.month))
    months.append((now.year, now.month))

    station_records = load_station_records()
    client = ObsdlBrowserClient()
    try:
        for year, month in months:
            end_day = days_in_month(year, month)
            if (year, month) == (now.year, now.month):
                end_day = now.day - 1
            if end_day < 1:
                logger.info('skip %04d-%02d because there is no completed day yet', year, month)
                continue

            pending = [
                record
                for record in station_records
                if should_refresh_recent_file(month_file_path(record.station_id, year, month), year, month, max_age_hours)
            ]
            logger.info('recent refresh target for %04d-%02d: %s stations', year, month, len(pending))
            for batch in chunked(pending, batch_size):
                if datetime.now() >= stop_at:
                    logger.info('time limit reached; stopping recent refresh')
                    return
                write_month_batch(client, batch, year, month, end_day)
    finally:
        client.close()


if __name__ == '__main__':
    main()
