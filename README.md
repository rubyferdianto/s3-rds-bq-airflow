# S3-RDS-BQ-Airflow Project

This project demonstrates a comprehensive data pipeline with multiple import workflows into AWS RDS MySQL database.

## 🗂️ Project Structure

```
s3-rds-bq-airflow/
├── main.py                       # 🎯 Main orchestrator (runs all components)
├── CSV-RDS/                     # 📁 RDS CSV import workflows
│   ├── s3-to-rds.py             # 📥 S3 to RDS import workflow (pandas-based)
│   ├── csv-to-rds-via-s3.py     # 📥 Local CSV to RDS import workflow
│   ├── create-rds-instance.py   # 🔧 Automated RDS instance creation
│   └── backup/                  # 📁 Backup of old scripts
├── csv-to-rds/                  # 📥 Local CSV staging folder
├── csv-imported-to-rds/         # 📁 Local CSV completed folder
├── RDS-BQ/                      # 📁 RDS to BigQuery workflows
├── .env                         # 🔐 Database credentials (gitignored)
├── .env.example                 # 📋 Template for environment variables
├── requirements-bec.yaml        # 🐍 Conda environment specification
└── README.md                    # 📖 This file
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
# Edit .env with your actual AWS RDS MySQL credentials
```

### 3. Create RDS Instance (if needed)
```bash
# Create RDS MySQL instance with VPC and security groups
python CSV-RDS/create-rds-instance.py
```

### 4. Run Complete Pipeline
```bash
# Run the main orchestrator (recommended)
python main.py

# This will execute:
# 0. Database connectivity check
# 1. S3 to RDS import (from s3://bec-bucket-aws/s3-to-rds/)
# 2. Local CSV to RDS import (from ./csv-to-rds/)
```

### 5. Individual Components
```bash
# Database check only
python main.py  # (includes connectivity check)

# S3 to RDS import only
python CSV-RDS/s3-to-rds.py

# Local CSV to RDS import only
python CSV-RDS/csv-to-rds-via-s3.py
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
- **Step 0**: Database connectivity check  
- **Step 1**: S3 import → RDS (pandas-based import)
- **Step 2**: Local CSV import → RDS

### Option 2: S3 Import Workflow
1. **📥 Upload Files**: Place CSV files in S3 bucket `s3://bec-bucket-aws/s3-to-rds/`
2. **▶️ Run Import**: Execute `python CSV-RDS/s3-to-rds.py`
3. **✅ Auto Processing**: Script imports to RDS using pandas and moves files to `s3-imported-to-rds/`

### Option 3: Local CSV Import Workflow
1. **📥 Stage Files**: Place CSV files in `csv-to-rds/` folder
2. **▶️ Run Import**: Execute `python CSV-RDS/csv-to-rds-via-s3.py`
3. **✅ Auto Processing**: Script imports to RDS and moves files to `csv-imported-to-rds/`

## 🛠️ Available Scripts

### Core Pipeline Components:
- **`main.py`** - Main orchestrator (runs all components in sequence)
- **`CSV-RDS/create-rds-instance.py`** - Automated RDS instance creation with VPC setup
- **`CSV-RDS/s3-to-rds.py`** - S3 to RDS import workflow with pandas-based processing
- **`CSV-RDS/csv-to-rds-via-s3.py`** - Local CSV to RDS import workflow

### Utility Scripts:
- **`show-storage.py`** - Show database storage details

## ✨ Key Features

- ✅ **Automated Pipeline** - Complete end-to-end workflow with one command
- ✅ **Dual Import Methods** - Both S3 and local CSV import capabilities
- ✅ **File Management** - Automatic file organization post-import
- ✅ **RDS Infrastructure** - Automated RDS instance creation with VPC
- ✅ **Error Handling** - Robust error management and logging
- ✅ **Progress Tracking** - Real-time import progress
- ✅ **MySQL Compatibility** - Optimized for AWS RDS MySQL
- ✅ **Cost Effective** - Uses standard RDS instead of Aurora for cost optimization
- ✅ **Pandas Integration** - Reliable pandas-based data processing

## 📈 Original Project Plan

1. Python will check andy CSV into S3 under bucket "BEC-BUCKET-AWS\S3-TO-RDS"
2. Python will also check if any CSV files ready to imported in local folder "CSV-TO-RDS"
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

- **Connection Issues**: Run `python main.py` to check database connectivity
- **Missing Tables**: Re-run the appropriate import script
- **Environment Issues**: Recreate conda environment with `conda env create -f requirements-bec.yaml`
- **Credentials**: Verify `.env` file configuration with RDS_* variables

## 💰 Cost Optimization

This project has been optimized for cost by switching from Aurora to standard RDS MySQL:
- **Aurora**: More expensive but offers advanced features like S3 integration
- **RDS MySQL**: Cost-effective alternative using pandas-based processing
- **Free Tier**: Uses db.t3.micro instances eligible for AWS free tier

---
*Project Status: Successfully implemented dual CSV import workflows to AWS RDS MySQL with automated pipeline orchestration and cost optimization*
