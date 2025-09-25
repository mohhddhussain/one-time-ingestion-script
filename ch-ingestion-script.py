import pandas as pd
from clickhouse_connect import get_client

# Connect
client = get_client(
    host="localhost",
    port=8124,
    username="default",
    password="",
    database=""
)

def clean_dataframe(df, table_schema):
    """
    - Convert datetime columns properly
    """
    
    for col, dtype in table_schema.items():
        if "Date" in dtype:
            df[col] = pd.to_datetime(df[col], errors="coerce")
        elif "String" in dtype:
            df[col] = df[col].fillna("").astype(str)
        elif "Int" in dtype or "Float" in dtype:
            df[col] = df[col].fillna(0)
    return df

def load_csv(path, table):
    try:
        # Get ClickHouse table schema
        schema = client.query(f"DESCRIBE TABLE {table}").result_rows
        schema_map = {row[0]: row[1] for row in schema}  # {col: type}

        # Read CSV (handles BOM automatically)
        df = pd.read_csv(path)

        # Clean dataframe (only datetime handling)
        df = clean_dataframe(df, schema_map)

        # Insert into CH
        client.insert_df(table, df)
        print(f"✅ Loaded {path} → {table} ({len(df)} rows)")
    except Exception as e:
        print(f"❌ Failed {path} → {table} : {e}")


files_to_tables = {
    "/home/hussain/Desktop/ch_tables/prod.digger_starlink_usage_v2.csv": "prod.digger_starlink_usage_v2",
    "/home/hussain/Desktop/ch_tables/prod.digger_starlink_usage_v2_time_series.csv": "prod.digger_starlink_usage_v2_time_series",
    "/home/hussain/Desktop/ch_tables/prod.agg_starlink_usage_v2_time_series_diff.csv": "prod.agg_starlink_usage_v2_time_series_diff",
    "/home/hussain/Desktop/ch_tables/prod.digger_starlink_service_lines_terminals.csv": "prod.digger_starlink_service_lines_terminals",
    "/home/hussain/Desktop/ch_tables/prod.digger_starlink_telemetry_agg_1day.csv": "prod.digger_starlink_telemetry_agg_1day",
    "/home/hussain/Desktop/ch_tables/prod.digger_starlink_service_lines.csv": "prod.digger_starlink_service_lines",
    "/home/hussain/Desktop/ch_tables/prod.digger_starlink_accounts.csv": "prod.digger_starlink_accounts",
    "/home/hussain/Desktop/ch_tables/prod.digger_starlink_credentials.csv": "prod.digger_starlink_credentials",
    "/home/hussain/Desktop/ch_tables/prod.digger_starlink_serviceline_subscriptions.csv": "prod.digger_starlink_serviceline_subscriptions",
    "/home/hussain/Desktop/ch_tables/prod.agg_starlink_usage_asset_ch.csv": "prod.agg_starlink_usage_asset_ch",
    "/home/hussain/Desktop/ch_tables/prod.digger_starlink_service_addresses.csv": "prod.digger_starlink_service_addresses",
    "/home/hussain/Desktop/ch_tables/prod.digger_starlink_subscriptions.csv": "prod.digger_starlink_subscriptions",
    "/home/hussain/Desktop/ch_tables/prod.digger_starlink_custom_plans.csv": "prod.digger_starlink_custom_plans",
    "/home/hussain/Desktop/ch_tables/prod.digger_starlink_available_products.csv": "prod.digger_starlink_available_products",
    "/home/hussain/Desktop/ch_tables/prod.digger_starlink_service_line_data_blocks.csv": "prod.digger_starlink_service_line_data_blocks",
    "/home/hussain/Desktop/ch_tables/prod.digger_starlink_current_billing_usage.csv": "prod.digger_starlink_current_billing_usage",
    "/home/hussain/Desktop/ch_tables/prod.digger_starlink_service_line_geolocation.csv": "prod.digger_starlink_service_line_geolocation",
    "/home/hussain/Desktop/ch_tables/prod.digger_starlink_user_terminals.csv": "prod.digger_starlink_user_terminals",
    "/home/hussain/Desktop/ch_tables/prod.digger_starlink_user_terminal_status.csv": "prod.digger_starlink_user_terminal_status",
    "/home/hussain/Desktop/ch_tables/prod.helper_1h_edge_starlink_terminal.csv": "prod.helper_1h_edge_starlink_terminal",
    "/home/hussain/Desktop/ch_tables/prod.edge_interfaces.csv": "prod.edge_interfaces",
    "/home/hussain/Desktop/ch_tables/prod.digger_starlink_telemetry_agg_last_values.csv": "prod.digger_starlink_telemetry_agg_last_values",
    "/home/hussain/Desktop/ch_tables/prod.starlink_service_line_last_location_with_kpi.csv": "prod.starlink_service_line_last_location_with_kpi",
    "/home/hussain/Desktop/ch_tables/prod.digger_starlink_telemetry.csv": "prod.digger_starlink_telemetry",
    "/home/hussain/Desktop/ch_tables/prod.digger_starlink_telemetry_ip.csv": "prod.digger_starlink_telemetry_ip",
    "/home/hussain/Desktop/ch_tables/prod.kapture_vessels.csv": "prod.kapture_vessels",
    "/home/hussain/Desktop/ch_tables/prod.kapture_distribution_partners.csv": "prod.kapture_distribution_partners",
    "/home/hussain/Desktop/ch_tables/prod.digger_starlink_quotas.csv": "prod.digger_starlink_quotas",
    "/home/hussain/Desktop/ch_tables/prod.digger_starlink_availability_alert.csv": "prod.digger_starlink_availability_alert",
    "/home/hussain/Desktop/ch_tables/prod.digger_starlink_geolocations.csv": "prod.digger_starlink_geolocations"
}

# Loop through all files
for path, table in files_to_tables.items():
    load_csv(path, table)

