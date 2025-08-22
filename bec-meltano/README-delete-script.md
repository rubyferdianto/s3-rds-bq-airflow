# RDS Cleanup Script Documentation

## delete-rds-after-load.py

This script is designed to safely clean up (truncate) RDS MySQL tables after successful BigQuery imports via Meltano pipeline.

### Features

✅ **Safe Verification**: Verifies data exists in BigQuery before deleting from RDS
✅ **Row Count Matching**: Compares row counts between RDS and BigQuery
✅ **Multiple Modes**: Dry run, verify-only, and force modes
✅ **Selective Cleanup**: Clean specific tables or all tables
✅ **Transaction Safety**: Uses database transactions for safe operations
✅ **Comprehensive Logging**: Detailed logs with cleanup summary

### Usage

```bash
# Basic usage - clean all tables after verification
python delete-rds-after-load.py

# Dry run - see what would be deleted
python delete-rds-after-load.py --dry-run

# Verify only - check BigQuery data without deleting
python delete-rds-after-load.py --verify-only

# Clean specific table
python delete-rds-after-load.py --table olist_orders_dataset

# Force cleanup without verification (DANGEROUS)
python delete-rds-after-load.py --force
```

### Safety Features

1. **BigQuery Verification**: Checks if data exists in BigQuery before deletion
2. **Row Count Comparison**: Ensures BigQuery has at least 90% of RDS row count
3. **Transaction Safety**: Uses database transactions to prevent partial failures
4. **Dry Run Mode**: Test mode to see what would be deleted
5. **Comprehensive Logging**: All operations are logged with timestamps

### Integration with Meltano

This script is designed to run after Meltano ETL pipeline:

1. **Meltano Pipeline**: Extracts from RDS → Loads to BigQuery
2. **Verification**: Script verifies successful BigQuery import
3. **Cleanup**: Script truncates RDS tables to free space

### Configuration

The script uses environment variables from `.env` file:

- `MYSQL_HOST`: RDS MySQL hostname
- `MYSQL_PORT`: RDS MySQL port (default: 3306)
- `MYSQL_USERNAME`: RDS MySQL username
- `MYSQL_PASSWORD`: RDS MySQL password
- `MYSQL_DATABASE`: RDS MySQL database name
- `GOOGLE_APPLICATION_CREDENTIALS_JSON`: BigQuery service account JSON
- `BQ_DATASET`: BigQuery dataset name (default: bec_bq)

### Example Output

```
🧹 RDS CLEANUP AFTER BIGQUERY IMPORT
============================================================
✅ MySQL connected: bec-db-rds.amazonaws.com/bec-db-rds
✅ BigQuery connected: infinite-byte-458600-a8
📊 Target dataset: infinite-byte-458600-a8.bec_bq

📋 Found 9 tables in RDS:
   • olist_orders_dataset: 99,441 rows (21.55 MB)
   • olist_products_dataset: 32,951 rows (3.52 MB)
   • olist_sellers_dataset: 3,095 rows (0.30 MB)

🔄 Processing table: olist_orders_dataset
📊 RDS rows: 99,441
✅ BigQuery table olist_orders_dataset: 99,441 rows found
✅ Successfully truncated olist_orders_dataset (99,441 rows deleted)

📊 CLEANUP SUMMARY
   Total tables: 9
   Verified in BigQuery: 5
   Cleaned tables: 5
   Skipped tables: 4
   Duration: 0:00:15.234567
============================================================
```

### Error Handling

The script handles various error conditions:

- Missing BigQuery data
- Row count mismatches
- Database connection failures
- Transaction rollbacks
- Invalid configurations

### Best Practices

1. **Always test with --dry-run first**
2. **Use --verify-only to check data integrity**
3. **Run after successful Meltano pipeline completion**
4. **Monitor logs for any warnings or errors**
5. **Keep backups of critical data**

### Automation

This script can be integrated into your pipeline automation:

```bash
# In your Meltano pipeline or workflow
meltano run tap-mysql target-bigquery && \
python delete-rds-after-load.py --verify-only && \
python delete-rds-after-load.py
```
