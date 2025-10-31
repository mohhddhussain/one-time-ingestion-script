import random
from datetime import datetime, timedelta
import clickhouse_connect

# Connect to ClickHouse
ch_client = clickhouse_connect.get_client(
    host="localhost",
    port=8124,
    username="default",
    password="",
)

tables_info = {
    "prod.digger_starlink_usage_v2": {"monthly_rows": 222833, "time_col": "date"},
    "prod.digger_starlink_usage_v2_time_series": {"monthly_rows": 2990072, "time_col": "query_time"},
    "prod.agg_starlink_usage_v2_time_series_diff": {"monthly_rows": 2925632, "time_col": "date"},
    "prod.digger_starlink_telemetry_agg_1day": {"monthly_rows": 231732, "time_col": "ts"},
    "prod.digger_starlink_telemetry": {"monthly_rows": 1179209640, "time_col": "ts"},
    "prod.digger_starlink_telemetry_ip": {"monthly_rows": 56667095, "time_col": "ts"},
    "prod.digger_starlink_geolocations": {"monthly_rows": 2531922, "time_col": "ts"}
}

def rows_per_day(monthly_rows):
    return monthly_rows / 30

def get_existing_row_count(table):
    return ch_client.query(f"SELECT count(*) FROM {table}").result_rows[0][0]

def fetch_sample_rows(table, limit=500):
    return ch_client.query(f"SELECT * FROM {table} LIMIT {limit}").result_rows

def backfill_table(table, target_rows):
    print(f"Backfilling {table}")
    current_rows = get_existing_row_count(table)
    rows_needed = target_rows - current_rows
    print(f"Need {rows_needed} rows")

    if rows_needed <= 0:
        print(f"Already at or above target rows")
        return

    time_col = tables_info[table]["time_col"]
    col_names = [col[0] for col in ch_client.query(f"DESCRIBE TABLE {table}").result_rows]
    ts_idx = col_names.index(time_col)

    max_ts = ch_client.query(f"SELECT max({time_col}) FROM {table}").result_rows[0][0]
    if not max_ts:
        max_ts = datetime.utcnow()

    inserted = 0
    batch_size = 500  # Adjust for performance

    while inserted < rows_needed:
        rows_to_insert = min(batch_size, rows_needed - inserted)
        sample_rows = fetch_sample_rows(table, rows_to_insert)
        backfilled_rows = []

        for i, row in enumerate(sample_rows):
            row = list(row)
            days_back = (inserted + i) % 90
            seconds_offset = random.randint(0, 3600)
            row[ts_idx] = max_ts - timedelta(days=days_back, seconds=seconds_offset)
            backfilled_rows.append(row)

        ch_client.insert(table, backfilled_rows, column_names=col_names)
        inserted += len(backfilled_rows)
        print(f"Inserted {inserted}/{rows_needed} rows", end="\r")

    print(f"\nFinished backfilling {table}")

# Backfill all tables to 90-day target
for table, info in tables_info.items():
    target_rows = info["monthly_rows"] * 3
    backfill_table(table, target_rows)
