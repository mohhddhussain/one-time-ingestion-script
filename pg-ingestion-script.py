import psycopg2
import os

DB_HOST = "localhost"   
DB_PORT = 5433
DB_USER = "postgres"
DB_PASS = "postgres"

# CSV → db/schema/table mapping
MAPPINGS = [
    ("K4", "K4", "distribution_partners", "/home/hussain/Desktop/Quantrail/postgres_data/K4.distribution_partners.csv"),
    ("K4", "K4", "vessels", "/home/hussain/Desktop/Quantrail/postgres_data/K4.vessels.csv"),
    ("starlink_cloud", "starlink_cloud", "digger_starlink_parent_pool_topups", "/home/hussain/Desktop/Quantrail/postgres_data/starlink_cloud.digger_starlink_parent_pool_topups.csv"),
    ("starlink_cloud", "starlink_cloud", "digger_starlink_pool_quota_actions", "/home/hussain/Desktop/Quantrail/postgres_data/starlink_cloud.digger_starlink_pool_quota_actions.csv"),
    ("starlink_cloud", "starlink_cloud", "digger_starlink_pool_quotas", "/home/hussain/Desktop/Quantrail/postgres_data/starlink_cloud.digger_starlink_pool_quotas.csv"),
    ("starlink_cloud", "starlink_cloud", "digger_starlink_service_lines", "/home/hussain/Desktop/Quantrail/postgres_data/service_lines_clean.csv"),
    ("starlink_cloud", "starlink_cloud", "digger_starlink_accounts", "/home/hussain/Desktop/Quantrail/postgres_data/starlink_cloud.digger_starlink_accounts.csv"),
    ("starlink_cloud", "starlink_cloud", "digger_starlink_availability_alert", "/home/hussain/Desktop/Quantrail/postgres_data/starlink_cloud.digger_starlink_availability_alert.csv"),
    ("starlink_cloud", "starlink_cloud", "digger_starlink_available_products", "/home/hussain/Desktop/Quantrail/postgres_data/starlink_cloud.digger_starlink_available_products.csv"),
    ("starlink_cloud", "starlink_cloud", "digger_starlink_credentials", "/home/hussain/Desktop/Quantrail/postgres_data/starlink_cloud.digger_starlink_credentials.csv"),
    ("starlink_cloud", "starlink_cloud", "digger_starlink_parent_pools", "/home/hussain/Desktop/Quantrail/postgres_data/starlink_cloud.digger_starlink_parent_pools.csv"),
    ("starlink_cloud", "starlink_cloud", "digger_starlink_quotas", "/home/hussain/Desktop/Quantrail/postgres_data/starlink_cloud.digger_starlink_quotas.csv"),
    ("starlink_cloud", "starlink_cloud", "digger_starlink_service_addresses", "/home/hussain/Desktop/Quantrail/postgres_data/starlink_cloud.digger_starlink_service_addresses.csv"),
    ("starlink_cloud", "starlink_cloud", "digger_starlink_service_line_data_blocks", "/home/hussain/Desktop/Quantrail/postgres_data/starlink_cloud.digger_starlink_service_line_data_blocks.csv"),
    ("starlink_cloud", "starlink_cloud", "digger_starlink_service_line_topups", "/home/hussain/Desktop/Quantrail/postgres_data/starlink_cloud.digger_starlink_service_line_topups.csv"),
    ("starlink_cloud", "starlink_cloud", "digger_starlink_serviceline_subscriptions", "/home/hussain/Desktop/Quantrail/postgres_data/starlink_cloud.digger_starlink_serviceline_subscriptions.csv"),
    ("starlink_cloud", "starlink_cloud", "digger_starlink_usage_v2", "/home/hussain/Desktop/Quantrail/postgres_data/starlink_cloud.digger_starlink_usage_v2.csv"),
    ("starlink_cloud", "starlink_cloud", "digger_starlink_user_terminals", "/home/hussain/Desktop/Quantrail/postgres_data/starlink_cloud.digger_starlink_user_terminals.csv")
]
def load_csv(db, schema, table, csv_file):
    try:
        conn = psycopg2.connect(
            dbname=db,
            user=DB_USER,
            password=DB_PASS,
            host=DB_HOST,
            port=DB_PORT
        )
        cur = conn.cursor()

        with open(csv_file, "r") as f:
            sql = f"COPY {schema}.{table} FROM STDIN WITH CSV HEADER"
            cur.copy_expert(sql, f)

        conn.commit()
        cur.close()
        conn.close()
        print(f"Loaded {csv_file} into {db}.{schema}.{table}")

    except Exception as e:
        print(f"Failed loading {csv_file} into {db}.{schema}.{table}")
        print(f"   Error: {e}")

if __name__ == "__main__":
    for db, schema, table, csv_file in MAPPINGS:
        if not os.path.exists(csv_file):
            print(f"File not found: {csv_file}, skipping...")
            continue
        load_csv(db, schema, table, csv_file)
