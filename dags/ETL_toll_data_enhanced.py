"""
ETL_toll_data_enhanced — portfolio version.

Same pipeline as ETL_toll_data.py, with two improvements:
  1. The three extractions run in PARALLEL (each reads a different source file).
  2. The consolidated and transformed files include a HEADER row,
     and the transformation leaves the header untouched.

Output: staging/transformed_data.csv (header + 9 columns).
Compatible with Apache Airflow 2.x and 3.x.
"""

from datetime import datetime, timedelta

try:
    from airflow.sdk import DAG
    from airflow.providers.standard.operators.bash import BashOperator
except ImportError:
    from airflow import DAG
    from airflow.operators.bash import BashOperator

STAGING = "/home/project/airflow/dags/finalassignment/staging"

HEADER = (
    "Rowid,Timestamp,Anonymized Vehicle number,Vehicle type,Number of axles,"
    "Tollplaza id,Tollplaza code,Type of Payment code,Vehicle Code"
)

default_args = {
    "owner": "SBM",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    "ETL_toll_data_enhanced",
    start_date=datetime(2026, 10, 1),
    schedule=timedelta(days=1),
    catchup=False,
    default_args=default_args,
    description="Toll data ETL with parallel extraction and header handling",
    tags=["etl", "bash", "portfolio"],
)

unzip_data = BashOperator(
    task_id="unzip_data",
    bash_command=f"cd {STAGING} && tar -xf ../tolldata.tgz",
    dag=dag,
)

extract_data_from_csv = BashOperator(
    task_id="extract_data_from_csv",
    bash_command=f"cd {STAGING} && cut -d ',' -f 1-4 ./vehicle-data.csv > csv_data.csv",
    dag=dag,
)

extract_data_from_tsv = BashOperator(
    task_id="extract_data_from_tsv",
    bash_command=f"cd {STAGING} && cut -f 5-7 ./tollplaza-data.tsv | tr -d '\\r' | tr '\\t' ',' > tsv_data.csv",
    dag=dag,
)

extract_data_from_fixed_width = BashOperator(
    task_id="extract_data_from_fixed_width",
    bash_command=f"cd {STAGING} && cut -c 59- ./payment-data.txt | tr ' ' ',' > fixed_width_data.csv",
    dag=dag,
)

# Header + merged rows, grouped so both go through a single redirection
consolidate_data = BashOperator(
    task_id="consolidate_data",
    bash_command=(
        f"cd {STAGING} && "
        f"(echo '{HEADER}'; paste -d ',' csv_data.csv tsv_data.csv fixed_width_data.csv) > extracted_data.csv"
    ),
    dag=dag,
)

# Line 1 (header) passes unchanged; lines 2+ get field 4 uppercased
transform_data = BashOperator(
    task_id="transform_data",
    bash_command=(
        f"cd {STAGING} && "
        f"awk -F',' 'NR==1{{print}} NR>1{{$4=toupper($4);print}}' OFS=',' extracted_data.csv > transformed_data.csv"
    ),
    dag=dag,
)

# Parallel extraction: unzip fans out to three tasks, which fan in to consolidate
unzip_data >> [extract_data_from_csv, extract_data_from_tsv, extract_data_from_fixed_width]
[extract_data_from_csv, extract_data_from_tsv, extract_data_from_fixed_width] >> consolidate_data
consolidate_data >> transform_data
