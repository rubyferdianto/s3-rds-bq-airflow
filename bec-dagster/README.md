# 🚀 Dagster Pipeline Directory

This directory contains all Dagster-related files for the S3-RDS-BigQuery ETL pipeline.

## 📁 Files

- **`dagster_pipeline.py`** - Main Dagster pipeline definition with all assets
- **`workspace.yaml`** - Dagster workspace configuration
- **`README.md`** - This documentation file

## 🎯 How to Run

### Method 1: Dagster Development Server (Recommended)
```bash
cd DAGSTER
python3 -m dagster dev --host 127.0.0.1 --port 3000
```
Then open: http://127.0.0.1:3000

### Method 2: Direct Python Execution
```bash
cd DAGSTER
python3 dagster_pipeline.py
```

## 📊 Pipeline Assets

1. **`_1_local_csv_to_s3`** - Ingestion: CSV to AWS S3
2. **`_2_s3_to_rds`** - Extraction: S3 to RDS 
3. **`_3_rds_to_bigquery`** - Loading: RDS to BigQuery
4. **`_4_process_datawarehouse`** - Transformation: DBT processing
5. **`_5_bigquery_to_visualization`** - Visualization: BigQuery to Dashboard

## ⚙️ Configuration

- Environment variables are loaded from `../.env` (parent directory)
- CSV files are read from `../CSV-RDS/csv-to-rds/`
- All subprocess calls execute from the parent directory

## 🔧 Dependencies

Make sure you have installed:
```bash
pip install dagster dagster-webserver python-dotenv
```

Or use the conda environment:
```bash
conda env create -f ../requirements-bec.yaml
conda activate bec
```
