# S3-RDS-BigQuery Complete Data Pipeline

A comprehensive end-to-end data pipeline that transfers CSV data from AWS S3 and local sources to RDS MySQL, then to Google BigQuery for analytics. Built with **Meltano ELT framework** for production-ready deployment.

## 🎯 Complete Data Flow

```
CSV Files (Local/S3) → AWS RDS MySQL → Google BigQuery
        ↓                    ↓              ↓
   📂 Source Data      🗄️ Staging DB    📊 Analytics
        │                    │              │
        └── Meltano ELT ──────┴──────────────┘
```

## 📋 Requirements

### Python Version
- **Python 3.11** (Required for Meltano compatibility)
- **Not compatible with Python 3.13+** (dependency conflicts)

### Recommended Setup
- **Production**: Meltano ELT framework (default)
- **Development**: Direct Python approach (fallback)
- **Orchestration**: Dagster for workflow management

## 🗂️ Current Project Structure

```
s3-rds-bq-dagster/
├── main.py                       # 🎯 Complete pipeline orchestrator
├── bec-aws-bq/                   # 📁 AWS & BigQuery workflows
│   ├── setup-database.py         # 🔧 Database setup and configuration
│   ├── s3-to-rds.py              # 📥 S3 to RDS direct import
│   ├── rds-to-bq.py              # 📤 RDS to BigQuery transfer
│   ├── csv-to-s3.py              # � Local CSV to S3 upload
│   ├── verify-bigquery.py        # ✅ BigQuery data verification
│   └── csv-imported-to-rds/      # 📂 Processed CSV files
├── bec-meltano/                  # 📁 Production Meltano ELT pipeline
│   ├── meltano.yml               # ⚙️ Meltano configuration
│   ├── rds-to-bq-meltano.py      # 🚀 Meltano pipeline runner
│   ├── delete-rds-after-load.py  # 🧹 RDS cleanup after transfer
│   ├── meltano-post-hook.py      # 🔗 Post-transfer automation
│   ├── plugins/                  # � Meltano extractors & loaders
│   └── .meltano/                 # �️ Meltano state & metadata
├── bec-dagster/                  # 🎼 Orchestration framework
│   ├── dagster_pipeline.py       # 🎯 Dagster asset definitions
│   ├── start_dagster.sh          # 🌐 Dagster web UI launcher
│   └── workspace.yaml            # ⚙️ Dagster configuration
├── .env                          # 🔐 Environment configuration (gitignored)
├── .env.example                  # 📋 Environment template
├── requirements-bec.yaml         # 🐍 Conda environment specification (ONLY requirements file)
└── README.md                     # 📖 This file
```

## 🚀 Quick Start - Complete Pipeline

### 1. Environment Setup

**Conda Environment (Recommended & Only Option)**
```bash
# Create conda environment with Python 3.11 and all dependencies
conda env create -f requirements-bec.yaml
conda activate bec

# Verify installation
python --version  # Should show 3.11.x
meltano --version # Should show 3.7.8+
```

### 2. Configure Environment Variables
```bash
# Copy template and edit with your credentials
cp .env.example .env
nano .env  # Add your database and cloud credentials
```

### 3. Run Complete Pipeline
```bash
# Run all stages (database setup → CSV import → BigQuery transfer)
python main.py

# Or run individual stages
python main.py --stage csv-s3    # CSV to S3 upload
python main.py --stage s3-rds    # S3 to RDS import
python main.py --stage rds-bq    # RDS to BigQuery transfer
```

## 🎼 Orchestration & Execution Options

### Dagster (Recommended for Development & Monitoring)
```bash
# Start Dagster web UI
cd bec-dagster/
./start_dagster.sh

# Access web interface at http://127.0.0.1:3000
```

### Meltano ELT (Production)
```bash
# Production approach with Meltano
cd bec-meltano/
python rds-to-bq-meltano.py

# With automatic RDS cleanup after BigQuery transfer
python rds-to-bq-meltano.py --enable-cleanup
```

### Direct Python (Development/Testing)
```bash
# Direct approach for quick testing
cd bec-aws-bq/
python rds-to-bq.py
```

## 🐳 Docker Deployment

The pipeline is optimized for Docker deployment using Meltano:

```bash
cd bec-meltano/
# Note: Docker support available through Meltano containerization
meltano run tap-mysql target-bigquery
```

## ⚙️ Configuration

### Environment Variables (.env)
```bash
# MySQL/RDS Configuration
MYSQL_HOST=your-rds-endpoint.region.rds.amazonaws.com
MYSQL_USERNAME=your_username
MYSQL_PASSWORD=your_password
MYSQL_DATABASE=your_database_name

# Google Cloud Configuration
GCP_PROJECT=your-project-id
BQ_DATASET=your_dataset_name
GOOGLE_APPLICATION_CREDENTIALS_JSON='{"type": "service_account", ...}'

# AWS Configuration
AWS_ACCESS_KEY_ID=your_access_key
AWS_SECRET_ACCESS_KEY=your_secret_key
S3_BUCKET_NAME=your-s3-bucket

# Pipeline Configuration
USE_MELTANO=true  # Use Meltano ELT (recommended)
RDS_CLEANUP_AFTER_TRANSFER=true  # Clean RDS after BigQuery transfer
```

## 🔧 Development Notes

### Python Version Compatibility
- **Python 3.11**: ✅ Fully supported (recommended)
- **Python 3.12**: ⚠️ Limited support (some dependency issues)
- **Python 3.13+**: ❌ Not supported (major dependency conflicts)

### Production vs Development
- **Production**: Use Meltano ELT framework (`bec-meltano/`) for robust, containerized deployment
- **Development**: Use direct Python approach (`bec-aws-bq/`) for quick testing
- **Orchestration**: Dagster (`bec-dagster/`) provides workflow management and monitoring
- **CI/CD**: Meltano provides better logging, state management, and error handling

### RDS Cleanup Feature
The pipeline includes automated RDS cleanup after successful BigQuery transfer:
- **Manual execution**: `python delete-rds-after-load.py`
- **Automatic**: Triggered by Meltano post-hooks after successful transfer
- **Verification**: Compares row counts between RDS and BigQuery before cleanup
- **Safety**: Only deletes RDS data if BigQuery verification passes

### Troubleshooting
1. **Meltano installation issues**: Ensure Python 3.11 is active
2. **BigQuery authentication**: Check GOOGLE_APPLICATION_CREDENTIALS_JSON format
3. **RDS connection**: Verify security groups allow your IP address
4. **Empty tables**: Check CSV files are present and S3 import completed
5. **Hook execution**: Check `bec-meltano/post_hook.log` for cleanup logs

## 📊 Pipeline Features

- ✅ **Automated database setup and configuration**
- ✅ **Resilient S3 to RDS import with file movement tracking**
- ✅ **Production-ready Meltano ELT framework with post-hooks**
- ✅ **Automated RDS cleanup after successful BigQuery transfer**
- ✅ **Dagster orchestration with web UI monitoring**
- ✅ **Comprehensive error handling and logging**
- ✅ **Environment-based configuration**
- ✅ **Data verification and integrity checks**
- ✅ **State management and incremental updates**
- ✅ **Multiple execution modes (production/development)**

## 🗂️ Key Components

### Data Flow Scripts
- **`bec-aws-bq/s3-to-rds.py`**: S3 to RDS MySQL import
- **`bec-aws-bq/rds-to-bq.py`**: Direct RDS to BigQuery transfer
- **`bec-meltano/rds-to-bq-meltano.py`**: Production Meltano pipeline

### Automation & Cleanup
- **`bec-meltano/delete-rds-after-load.py`**: RDS cleanup engine with safety checks
- **`bec-meltano/meltano-post-hook.py`**: Automated post-transfer hooks
- **`bec-aws-bq/verify-bigquery.py`**: BigQuery data verification

### Orchestration
- **`bec-dagster/dagster_pipeline.py`**: Workflow orchestration assets
- **`main.py`**: Central pipeline orchestrator

## 🤝 Contributing

1. Ensure Python 3.11 conda environment: `conda activate bec`
2. Install environment: `conda env create -f requirements-bec.yaml`
3. Test with both Meltano and direct approaches
4. Update documentation for any new features

## 📄 License

This project is licensed under the MIT License - see the LICENSE file for details.
