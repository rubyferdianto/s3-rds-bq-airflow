# S3-R```
s3-rds-bq-airflow/
├── main.py                       # 🎯 Main orchestrator (runs all components)
├── setup_database_simple.py      # 🔧 Database setup (creates database if needed)
├── check_databases.py            # 👀 Database connectivity check
├── s3_to_rds.py                  # 📥 S3 to RDS import workflow
├── csv_to_rds.py                 # 📥 Local CSV to RDS import workflow
├── csv_to_rds/                   # 📥 Local CSV staging folder
├── csv_imported_to_rds/          # 📁 Local CSV completed folder
├── s3-to-rds/                    # 📥 S3 staging folder (on S3)
├── s3-imported-to-rds/           # 📁 S3 completed folder (on S3)
└── ...other files
```Project

This project demonstrates a comprehensive data pipeline with multiple import workflows into AWS RDS MySQL database.

## 🗂️ Project Structure

```
s3-rds-bq-airflow/
├── main.py                       # 🎯 Main orchestrator (runs all components)
├── setup_database_simple.py      # � Database setup (creates database if needed)
├── check_databases.py            # 👀 Database connectivity check
├── s3_to_rds.py                  # 📥 S3 to RDS import workflow
├── csv_to_rds.py                 # 📥 Local CSV to RDS import workflow
├── load_csv_to_rds.py            # ⭐ Legacy: Combined script
├── csv_to_rds/                   # 📥 Local CSV staging folder
├── csv_imported_to_rds/          # � Local CSV completed folder
├── s3-to-rds/                    # � S3 staging folder (on S3)
├── s3-imported-to-rds/           # 📁 S3 completed folder (on S3)
└── ...other files
```

## 🚀 Quick Start

### 1. Environment Setup
```bash
# Create conda environment
conda env create -f requirements-bec.yaml
conda activate bec
```

### 2. Database Configuration
```bash
# Copy and edit environment variables
cp .env.example .env
# Edit .env with your actual AWS RDS credentials
```

### 3. Run Complete Pipeline
```bash
# Run the main orchestrator (recommended)
python main.py

# This will execute:
# 0. Database setup (creates database if needed)
# 1. Database connectivity check
# 2. S3 to RDS import (from s3://bec-bucket-aws/s3-to-rds/)
# 3. Local CSV to RDS import (from ./csv_to_rds/)
```

### 4. Individual Components
```bash
# Database setup only
python setup_database_simple.py

# Database check only
python check_databases.py

# S3 to RDS import only
python s3_to_rds.py

# Local CSV to RDS import only
python csv_to_rds.py
```

## 📊 Database Status

Successfully loaded **9 tables** with **451,322+ total rows**:

| Table | Rows | Description |
|-------|------|-------------|
| olist_customers_dataset | 99,441 | Customer information |
| olist_geolocation_dataset | 1,000,163 | Geographic data |
| olist_sellers_dataset | 3,095 | Seller information |
| olist_orders_dataset | 99,441 | Order details |
| olist_order_items_dataset | 112,650 | Order line items |
| olist_order_payments_dataset | 103,886 | Payment information |
| olist_order_reviews_dataset | 99,224 | Customer reviews |
| olist_products_dataset | 32,951 | Product catalog |
| product_category_name_translation | 71 | Category translations |

## � **Workflow Options**

### Option 1: Complete Pipeline (Recommended)
```bash
python main.py
```
- Executes all steps in sequence
- Database check → S3 import → Local CSV import

### Option 2: S3 Import Workflow
1. **📥 Upload Files**: Place CSV files in S3 bucket `s3://bec-bucket-aws/s3-to-rds/`
2. **▶️ Run Import**: Execute `python s3_to_rds.py`
3. **✅ Auto Processing**: Script imports to RDS and moves files to `s3-imported-to-rds/`

### Option 3: Local CSV Import Workflow
1. **� Stage Files**: Place CSV files in `csv_to_rds/` folder
2. **▶️ Run Import**: Execute `python csv_to_rds.py`
3. **✅ Auto Processing**: Script imports to RDS and moves files to `csv_imported_to_rds/`

## 🛠️ Available Scripts

### Core Pipeline Components:
- **`main.py`** - Main orchestrator (runs all components in sequence)
- **`setup_database_simple.py`** - Database setup and creation (runs first)
- **`check_databases.py`** - Database connectivity check  
- **`s3_to_rds.py`** - S3 to RDS import workflow (connects to existing database)
- **`csv_to_rds.py`** - Local CSV to RDS import workflow (connects to existing database)

### Utility Scripts:
- **`setup_database.py`** - Interactive database setup utility
- **`show_storage.py`** - Show storage usage and row counts

### Configuration:
- **`.env`** - Your database credentials
- **`requirements-bec.yaml`** - Python environment dependencies

## 📋 Environment Variables

Required in `.env` file:
```bash
# AWS Configuration
AWS_ACCESS_KEY_ID=your_access_key
AWS_SECRET_ACCESS_KEY=your_secret_key
AWS_DEFAULT_REGION=your_region

# MySQL/RDS Configuration  
MYSQL_HOST=your-rds-endpoint.amazonaws.com
MYSQL_DATABASE=your_database_name
MYSQL_USERNAME=your_username
MYSQL_PASSWORD=your_password
MYSQL_PORT=3306
```

## 🎯 Key Features

- ✅ **Automatic Database Setup** - Ensures database exists before loading
- ✅ **Controlled Import Process** - Only processes files in staging folder
- ✅ **File Management** - Automatically moves imported files to track completion
- ✅ **Direct CSV to RDS** - Loads directly from local CSV to RDS MySQL
- ✅ **No S3 Required** - Bypasses S3 for faster, simpler loading
- ✅ **Smart Duplicate Handling** - Replaces existing tables if re-imported
- ✅ **Progress Tracking** - Real-time loading progress
- ✅ **Error Handling** - Robust error management
- ✅ **Column Cleaning** - MySQL-compatible column names
- ✅ **Chunked Processing** - Handles large files efficiently

## 📈 Original Project Plan

1. Python will get the CSV files in local folder then import into S3.
2. Meltano will get from AWS S3 to AWS RDS
3. AWS RDS will transfer into GCP BigQuery
4. BigQuery process data analytic for factsales dimension
5. Visualisation get the factsales
6. Airflow will be used to monitor the process from steps 1 to 5

## 🔧 Current Implementation Status

✅ **Completed**: Direct CSV → MySQL RDS loading  
⏳ **Next**: Steps 3-6 (BigQuery, Analytics, Visualization, Airflow)

## 🔧 Troubleshooting

- **Connection Issues**: Run `python check_databases.py` to diagnose
- **Missing Tables**: Re-run `python load_local_csvs.py`
- **Environment Issues**: Recreate conda environment
- **Credentials**: Verify `.env` file configuration

---
*Project completed: Successfully migrated 9 CSV files to AWS RDS MySQL*

