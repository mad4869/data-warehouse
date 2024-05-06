#!/bin/bash

echo "========== Start dbt with Luigi Orchestration Process =========="

# Virtual Environment Path
VENV_PATH="/home/mad4869/Documents/pacmann/data-storage/data-warehouse/dwh-venv/bin/activate"

# Activate Virtual Environment
source "$VENV_PATH"

# Set Python script
PYTHON_SCRIPT="/home/mad4869/Documents/pacmann/data-storage/data-warehouse/elt.py"

# Get Current Date
current_datetime=$(date '+%d-%m-%Y_%H-%M')

# Append Current Date in the Log File
log_file="/home/mad4869/Documents/pacmann/data-storage/data-warehouse/logs/elt/elt_$current_datetime.log"

# Run Python Script and Insert Log
python "$PYTHON_SCRIPT" >> "$log_file" 2>&1

echo "========== End of dbt with Luigi Orchestration Process =========="