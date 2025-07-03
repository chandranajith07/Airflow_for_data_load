import pandas as pd
from google.cloud import bigquery

# ---- Config ----
csv_path = "C:/Users/lenovo/Downloads/csv_1.csv"
table_id = "deep-chimera-459105-q6.my_dataset.tb_1"

# ---- Step 1: Get BigQuery schema column names ----
client = bigquery.Client()
table = client.get_table(table_id)
bq_columns = [schema_field.name for schema_field in table.schema]

# ---- Step 2: Load CSV ----
df = pd.read_csv(csv_path)
csv_columns = list(df.columns)

# ---- Step 3: Normalize CSV columns (to lowercase, underscores) ----
normalized_csv_columns = [col.strip().lower().replace(" ", "_") for col in csv_columns]

# ---- Step 4: Try to match normalized columns with BigQuery schema ----
column_mapping = {}
for original, normalized in zip(csv_columns, normalized_csv_columns):
    if normalized in bq_columns:
        column_mapping[original] = normalized
    elif original in bq_columns:
        column_mapping[original] = original  # already good

# Rename if needed
df.rename(columns=column_mapping, inplace=True)

# Final check: keep only matching columns
df = df[[col for col in df.columns if col in bq_columns]]

# ---- Step 5: Load to BigQuery ----
job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")
job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
job.result()

print(f"✅ Loaded {job.output_rows} rows to {table_id}")
