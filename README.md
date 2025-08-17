# S3-RDS-BQ-Airflow Project

This project demonstrates a comprehensive data pipeline with multiple import workflows into AWS RDS MySQL database.

## 🗂️ Project Structure

```
s3-rds-bq-airflow/
├── main.py                       # 🎯 Main orchestrator (runs all components)
├── setup_database.py             # � Automated database setup
├── check_databases.py            # 👀 Database connectivity check
├── s3_to_rds.py                  # 📥 S3 to RDS import workflow
├── csv_to_rds.py                 # 📥 Local CSV to RDS import workflow
├── csv_to_rds/                   # 📥 Local CSV staging folder
├── csv_imported_to_rds/          # 📁 Local CSV completed folder
├── s3-to-rds/                    # 📥 S3 staging folder (on S3)
├── s3-imported-to-rds/           # 📁 S3 completed folder (on S3)
├── .env                          # 🔐 Database credentials (gitignored)
├── .env.example                  # 📋 Template for environment variables
├── requirements-bec.yaml         # 🐍 Conda environment specification
└── README.md                     # 📖 This file
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
python setup_database.py

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

## 🔄 **Workflow Options**

### Option 1: Complete Pipeline (Recommended)
```bash
python main.py
```
- **Step 0**: Database setup (creates database if needed)
- **Step 1**: Database connectivity check  
- **Step 2**: S3 import → RDS
- **Step 3**: Local CSV import → RDS

### Option 2: S3 Import Workflow
1. **📥 Upload Files**: Place CSV files in S3 bucket `s3://bec-bucket-aws/s3-to-rds/`
2. **▶️ Run Import**: Execute `python s3_to_rds.py`
3. **✅ Auto Processing**: Script imports to RDS and moves files to `s3-imported-to-rds/`

### Option 3: Local CSV Import Workflow
1. **📥 Stage Files**: Place CSV files in `csv_to_rds/` folder
2. **▶️ Run Import**: Execute `python csv_to_rds.py`
3. **✅ Auto Processing**: Script imports to RDS and moves files to `csv_imported_to_rds/`

## 🛠️ Available Scripts

### Core Pipeline Components:
- **`main.py`** - Main orchestrator (runs all components in sequence)
- **`setup_database.py`** - Automated database setup (runs first in pipeline)
- **`check_databases.py`** - Database connectivity check  
- **`s3_to_rds.py`** - S3 to RDS import workflow (connects to existing database)
- **`csv_to_rds.py`** - Local CSV to RDS import workflow (connects to existing database)

### Utility Scripts:
- **`show_storage.py`** - Show database storage details

## ✨ Key Features

- ✅ **Automated Pipeline** - Complete end-to-end workflow with one command
- ✅ **Dual Import Methods** - Both S3 and local CSV import capabilities
- ✅ **File Management** - Automatic file organization post-import
- ✅ **Database Auto-Setup** - Creates database if it doesn't exist
- ✅ **Error Handling** - Robust error management and logging
- ✅ **Progress Tracking** - Real-time import progress
- ✅ **MySQL Compatibility** - Optimized for AWS RDS MySQL

## 📈 Original Project Plan

1. Python will get the CSV files in local folder then import into S3.
2. Meltano will get from AWS S3 to AWS RDS
3. AWS RDS will transfer into GCP BigQuery
4. BigQuery process data analytic for factsales dimension
5. Visualisation get the factsales
6. Airflow will be used to monitor the process from steps 1 to 5

## 🔧 Current Implementation Status

✅ **Completed**: 
- Step 1: CSV → S3 and direct CSV → RDS loading
- Step 2: S3 → RDS import workflow

⏳ **Next**: Steps 3-6 (BigQuery, Analytics, Visualization, Airflow)

## 🔧 Troubleshooting

- **Connection Issues**: Run `python check_databases.py` to diagnose
- **Database Setup**: Use `python setup_database.py` for interactive setup
- **Missing Tables**: Re-run the appropriate import script
- **Environment Issues**: Recreate conda environment
- **Credentials**: Verify `.env` file configuration

---
*Project Status: Successfully implemented dual CSV import workflows to AWS RDS MySQL with automated pipeline orchestration*
