"""
ETL_toll_data — submission version (graded).

Collects road traffic data from three toll operators, each using a different
file format (CSV, TSV, fixed-width), consolidates it into one CSV file and
transforms the vehicle type to uppercase.

Output: staging/transformed_data.csv (no header, 9 columns).
Compatible with Apache Airflow 2.x and 3.x.
"""

from datetime import datetime, timedelta

# DAG and BashOperator: try Airflow 3 locations first, fall back to Airflow 2
try:
    from airflow.sdk import DAG
    from airflow.providers.standard.operators.bash import BashOperator
except ImportError:
    from airflow import DAG
    from airflow.operators.bash import BashOperator

# Shared working folder: every task reads and writes here
STAGING = "/home/project/airflow/dags/finalassignment/staging"

default_args = {
    "owner": "SBM",
    "email": ["test@test.com"],
    "email_on_failure": True,
    "email_on_retry": True,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    "ETL_toll_data",
    start_date=datetime(2026, 10, 1),
    schedule=timedelta(days=1),
    catchup=False,
    default_args=default_args,
    description="Apache Airflow Final Assignment",
)

# Unzip the source archive (stored one level above staging)
unzip_data = BashOperator(
    task_id="unzip_data",
    bash_command=f"cd {STAGING} && tar -xf ../tolldata.tgz",
    dag=dag,
)

# CSV source: Rowid, Timestamp, Anonymized Vehicle number, Vehicle type
extract_data_from_csv = BashOperator(
    task_id="extract_data_from_csv",
    bash_command=f"cd {STAGING} && cut -d ',' -f 1-4 ./vehicle-data.csv > csv_data.csv",
    dag=dag,
)

# TSV source: Number of axles, Tollplaza id, Tollplaza code
# Tabs -> commas, and Windows line endings (\r) removed
extract_data_from_tsv = BashOperator(
    task_id="extract_data_from_tsv",
    bash_command=f"cd {STAGING} && cut -f 5-7 ./tollplaza-data.tsv | tr '\\t' ',' | tr -d '\\r' > tsv_data.csv",
    dag=dag,
)

# Fixed-width source: Type of Payment code, Vehicle Code (from character 59)
extract_data_from_fixed_width = BashOperator(
    task_id="extract_data_from_fixed_width",
    bash_command=f"cd {STAGING} && cut -c 59- ./payment-data.txt | tr ' ' ',' > fixed_width_data.csv",
    dag=dag,
)

# Merge the three extracts side by side, line by line
consolidate_data = BashOperator(
    task_id="consolidate_data",
    bash_command=f"cd {STAGING} && paste -d ',' csv_data.csv tsv_data.csv fixed_width_data.csv > extracted_data.csv",
    dag=dag,
)

# Uppercase the vehicle type (field 4), keeping every other column intact
transform_data = BashOperator(
    task_id="transform_data",
    bash_command=f"cd {STAGING} && awk -F',' '{{$4=toupper($4);print}}' OFS=',' extracted_data.csv > transformed_data.csv",
    dag=dag,
)

# Task pipeline
unzip_data >> extract_data_from_csv >> extract_data_from_tsv >> extract_data_from_fixed_width >> consolidate_data >> transform_data
