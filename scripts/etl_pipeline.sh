#!/usr/bin/env bash
#
# etl_pipeline.sh — standalone version of the toll data ETL (no Airflow needed).
#
# Usage:
#   ./scripts/etl_pipeline.sh [STAGING_DIR] [ARCHIVE]
#   WITH_HEADER=true ./scripts/etl_pipeline.sh /path/to/staging /path/to/tolldata.tgz
#
# Defaults match the Airflow lab layout:
#   STAGING_DIR = /home/project/airflow/dags/finalassignment/staging
#   ARCHIVE     = <STAGING_DIR>/../tolldata.tgz

set -euo pipefail

STAGING="${1:-/home/project/airflow/dags/finalassignment/staging}"
ARCHIVE="${2:-$STAGING/../tolldata.tgz}"
WITH_HEADER="${WITH_HEADER:-false}"
HEADER="Rowid,Timestamp,Anonymized Vehicle number,Vehicle type,Number of axles,Tollplaza id,Tollplaza code,Type of Payment code,Vehicle Code"

log()  { echo "[$(date '+%H:%M:%S')] $*"; }
fail() { echo "ERROR: $*" >&2; exit 1; }

[ -d "$STAGING" ] || fail "staging directory not found: $STAGING"
[ -f "$ARCHIVE" ] || fail "archive not found: $ARCHIVE"

cd "$STAGING"

log "1/6 Unzipping $(basename "$ARCHIVE")"
tar -xf "$ARCHIVE"

for f in vehicle-data.csv tollplaza-data.tsv payment-data.txt; do
    [ -f "$f" ] || fail "expected source file missing after unzip: $f"
done

log "2/6 Extracting fields 1-4 from CSV"
cut -d ',' -f 1-4 vehicle-data.csv > csv_data.csv

log "3/6 Extracting fields 5-7 from TSV (tabs -> commas, CRLF -> LF)"
cut -f 5-7 tollplaza-data.tsv | tr -d '\r' | tr '\t' ',' > tsv_data.csv

log "4/6 Extracting payment and vehicle codes from fixed-width file"
cut -c 59- payment-data.txt | tr ' ' ',' > fixed_width_data.csv

log "5/6 Consolidating into extracted_data.csv (header: $WITH_HEADER)"
if [ "$WITH_HEADER" = "true" ]; then
    { echo "$HEADER"; paste -d ',' csv_data.csv tsv_data.csv fixed_width_data.csv; } > extracted_data.csv
else
    paste -d ',' csv_data.csv tsv_data.csv fixed_width_data.csv > extracted_data.csv
fi

log "6/6 Transforming vehicle type to uppercase"
if [ "$WITH_HEADER" = "true" ]; then
    awk -F',' 'NR==1{print} NR>1{$4=toupper($4);print}' OFS=',' extracted_data.csv > transformed_data.csv
else
    awk -F',' '{$4=toupper($4);print}' OFS=',' extracted_data.csv > transformed_data.csv
fi

log "Done: $STAGING/transformed_data.csv ($(wc -l < transformed_data.csv) lines)"
