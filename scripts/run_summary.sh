#!/bin/bash

# === Usage Help ===
if [ $# -lt 3 ]; then
    echo "Usage: $0 <country> <le_book> <pipeline_type> [business_date]"
    echo "Examples:"
    echo "  $0 KE 01 ra_summary 2025-04-30"    # Uses specified date
    echo "  $0 KE 01 bs_summary 2025-04-30"    # Uses specified date
    exit 1
fi

# === Initialize Conda ===
# Use system-wide conda path
source /opt/miniconda/etc/profile.d/conda.sh
conda activate crewai_env

# === Get arguments ===
COUNTRY="$1"
LE_BOOK="$2"

# Handle optional 4th argument
if [ $# -eq 3 ]; then
    BUSINESS_DATE=$(date +%Y-%m-%d)
    PIPELINE_TYPE="$3"
else
    PIPELINE_TYPE="$3"
    BUSINESS_DATE="$4"
fi

# === Validate Inputs ===
if [[ -z "$COUNTRY" || -z "$LE_BOOK" ]]; then
    echo "Error: Country and LE Book are required"
    exit 1
fi

if [[ ! "$BUSINESS_DATE" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}$ ]]; then
    echo "Error: Invalid business date format. Use YYYY-MM-DD"
    exit 1
fi

# === Run Python Script ===
python "/home/vision/app/NewsLetter/scripts/main.py" \
    --country "$COUNTRY" \
    --le_book "$LE_BOOK" \
    --business_date "$BUSINESS_DATE" \
    --pipeline_type "$PIPELINE_TYPE"

# === Handle Exit Code ===
EXIT_CODE=$?
if [ $EXIT_CODE -ne 0 ]; then
    echo "❌ Error: Script failed with exit code $EXIT_CODE"
else
    echo "✅ Success: Script completed"
fi

exit $EXIT_CODE
