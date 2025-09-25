import pandas as pd
import json
import re

csv_file = "/home/hussain/Desktop/Quantrail/postgres_data/starlink_cloud.digger_starlink_service_lines.csv"
df = pd.read_csv(csv_file)

def fix_email_ids(val):
    if pd.isna(val) or val.strip() == "":
        return None  # empty -> null
    try:
        # Attempt to fix simple pattern like nullnamenull:nullStarlinknull
        val = re.sub(r'nullnamenull', '"name"', val)
        val = re.sub(r'nullemailnull', '"email"', val)
        val = re.sub(r'null([^\s,}]+)null', r'"\1"', val)  # wrap values in quotes
        # Validate JSON
        json.loads(val)
        return val
    except Exception:
        return None  # fallback to null if still invalid

df['email_ids'] = df['email_ids'].apply(fix_email_ids)

# Save clean CSV
tmp_csv = "/home/hussain/Downloads/service_lines_clean.csv"
df.to_csv(tmp_csv, index=False)
