#!/bin/bash

CSV_BASE="/user/vehicle_market/tableau_csv"
LOCAL_DIR="$HOME/tableau_data"

mkdir -p "$LOCAL_DIR"

TABLES=(
"kpi_summary"
"manufacturer_summary"
"state_summary"
"monthly_summary"
"fuel_summary"
"segment_summary"
"condition_summary"
"title_summary"
"transmission_summary"
"state_manufacturer"
"manufacturer_condition"
)

for TABLE in "${TABLES[@]}"
do
    echo "Exporting $TABLE..."

    hdfs dfs -getmerge \
        "$CSV_BASE/$TABLE" \
        "$LOCAL_DIR/$TABLE.csv"

done