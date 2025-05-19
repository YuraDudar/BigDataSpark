#!/bin/bash
set -e 

echo "Starting CSV data import into mock_data table..."

CSV_DIR="/input_data_mount"

if ! ls -1qA "${CSV_DIR}"/*.csv >/dev/null 2>&1; then
    echo "No CSV files found in ${CSV_DIR}. Skipping data import."
    exit 0
fi

for csv_file in "${CSV_DIR}"/*.csv; do
  if [ -f "$csv_file" ]; then
    echo "Processing file: $csv_file"
    psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" \
         -c "\\copy mock_data FROM '${csv_file}' WITH (FORMAT CSV, HEADER TRUE, DELIMITER ',', ENCODING 'UTF8');"
    echo "Successfully imported data from $csv_file"
  else
    echo "Warning: $csv_file is not a valid file. Skipping."
  fi
done

echo "CSV data import finished successfully."

TOTAL_ROWS=$(psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" -t -c "SELECT COUNT(*) FROM mock_data;")
echo "Total rows in mock_data table: $TOTAL_ROWS"