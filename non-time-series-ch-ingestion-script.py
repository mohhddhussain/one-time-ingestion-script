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

# Tables with monthly counts & anchor date columns
tables_info = {
    "prod.digger_starlink_credentials": {"monthly_rows": 5, "time_col": "created_at"},
    #NO TIME COLUMN i-e no monthly count, mocked for full count
    "prod.digger_starlink_service_lines_terminals": {"monthly_rows": 57862, "time_col": "updated_at"}, 
    "prod.digger_starlink_service_line_geolocation": {"monthly_rows": 13047, "time_col": "ts"},
    "prod.digger_starlink_telemetry_agg_last_values": {"monthly_rows": 8482, "time_col": "last_ts"},
    #NO TIME COLUMN i-e no monthly count, mocked for full count
    "prod.starlink_service_line_last_location_with_kpi": {"monthly_rows": 84781, "time_col": "ts"},
}

def rows_per_day(monthly_rows):
    return max(1, int(monthly_rows / 30))  # Ensure at least 1/day if tiny

def get_existing_row_count(table):
    return ch_client.query(f"SELECT count(*) FROM {table}").result_rows[0][0]

def fetch_sample_rows(table, limit=500):
    return ch_client.query(f"SELECT * FROM {table} LIMIT {limit}").result_rows

def backfill_table(table, monthly_rows, time_col):
    print(f"Backfilling {table}")
    current_rows = get_existing_row_count(table)
    target_rows = monthly_rows * 3  
    rows_needed = target_rows - current_rows
    print(f"   ➡️  Need {rows_needed} rows")

    if rows_needed <= 0:
        print(f"Already at or above target rows")
        return

    # Fetch schema
    col_names = [col[0] for col in ch_client.query(f"DESCRIBE TABLE {table}").result_rows]
    ts_idx = col_names.index(time_col)

    # Anchor date
    max_ts = ch_client.query(f"SELECT max({time_col}) FROM {table}").result_rows[0][0]
    if not max_ts:
        max_ts = datetime.utcnow()

    inserted = 0
    batch_size = 500

    while inserted < rows_needed:
        rows_to_insert = min(batch_size, rows_needed - inserted)
        sample_rows = fetch_sample_rows(table, rows_to_insert)
        backfilled_rows = []

        for i, row in enumerate(sample_rows):
            row = list(row)

            # Spread across 90 days
            days_back = (inserted + i) % 90
            seconds_offset = random.randint(0, 86400)  # random time in a day
            row[ts_idx] = max_ts - timedelta(days=days_back, seconds=seconds_offset)

            backfilled_rows.append(row)

        # Insert into ClickHouse
        ch_client.insert(table, backfilled_rows, column_names=col_names)
        inserted += len(backfilled_rows)
        print(f"Inserted {inserted}/{rows_needed} rows", end="\r")

    print(f"\nFinished backfilling {table}")

# Backfill all tables
for table, info in tables_info.items():
    backfill_table(table, info["monthly_rows"], info["time_col"])
