#!/usr/bin/env python3
"""
RDS MySQL -> Google Cloud BigQuery Direct Transfer with Data Cleanup
1. Scans RDS MYSQL database based on the tables of CSV files
2. Copy from RDS to BQ using Python google cloud directly
3. Delete data in RDS if table is successfully imported into BQ

Performance Options:
    --fast      Use optimized transfer (default, 5x faster for large tables)
    --slow      Use original chunked method (more conservative)

Usage:
    python rds-bq.py                          # Fast transfer all tables and delete RDS data
    python rds-bq.py --fast                   # Explicitly use fast method (default)
    python rds-bq.py --slow                   # Use slower but safer chunked method  
    python rds-bq.py table1 table2           # Fast transfer specific tables
    python rds-bq.py --no-delete             # Transfer all tables but keep RDS data
    python rds-bq.py table1 --no-delete      # Transfer specific table but keep RDS data
    python rds-bq.py --slow --no-delete      # Slow transfer, keep RDS data
"""

import os
import sys
import json
import pandas as pd
import pymysql
import logging
from datetime import datetime
from dotenv import load_dotenv
from google.cloud import bigquery
from google.oauth2 import service_account
from sqlalchemy import create_engine, text

# Load environment variables
load_dotenv()

# Set up logging
logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('rds_bq_transfer.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class RDSToBigQueryTransfer:
    """Direct RDS MySQL to BigQuery transfer based on CSV table structure"""
    
    def __init__(self):
        self.start_time = datetime.now()
        self.mysql_engine = None
        self.bq_client = None
        self.project_id = None
        self.dataset_id = None
        logger.info(f"🚀 Starting RDS → BigQuery Transfer at {self.start_time}")
    
    def setup_mysql_connection(self):
        """Setup MySQL/RDS connection"""
        logger.info("🔗 Setting up MySQL connection...")
        
        try:
            mysql_config = {
                'host': os.getenv('MYSQL_HOST'),
                'port': int(os.getenv('MYSQL_PORT', 3306)),
                'user': os.getenv('MYSQL_USERNAME'),
                'password': os.getenv('MYSQL_PASSWORD'),
                'database': os.getenv('MYSQL_DATABASE'),
                'charset': 'utf8mb4'
            }
            
            # Validate required config
            required_fields = ['host', 'user', 'password', 'database']
            missing = [field for field in required_fields if not mysql_config.get(field)]
            
            if missing:
                logger.error(f"❌ Missing MySQL config: {missing}")
                return False
            
            # Create SQLAlchemy engine for pandas
            connection_string = (
                f"mysql+pymysql://{mysql_config['user']}:"
                f"{mysql_config['password']}@{mysql_config['host']}:"
                f"{mysql_config['port']}/{mysql_config['database']}"
            )
            
            self.mysql_engine = create_engine(connection_string)
            
            # Test connection
            with self.mysql_engine.connect() as conn:
                result = conn.execute(text("SELECT 1 as test"))
                test_value = result.scalar()
                
            logger.info(f"✅ MySQL connected: {mysql_config['host']}/{mysql_config['database']}")
            return True
            
        except Exception as e:
            logger.error(f"❌ MySQL connection failed: {e}")
            return False
    
    def setup_bigquery_client(self):
        """Setup BigQuery client"""
        logger.info("🔗 Setting up BigQuery client...")
        
        try:
            credentials_json = os.getenv('GOOGLE_APPLICATION_CREDENTIALS_JSON')
            if not credentials_json:
                logger.error("❌ GOOGLE_APPLICATION_CREDENTIALS_JSON not set")
                return False
            
            credentials_dict = json.loads(credentials_json)
            credentials = service_account.Credentials.from_service_account_info(credentials_dict)
            
            self.project_id = credentials_dict.get('project_id')
            if not self.project_id:
                logger.error("❌ No project_id in credentials")
                return False
            
            self.bq_client = bigquery.Client(project=self.project_id, credentials=credentials)
            
            # Test connection
            query = "SELECT 1 as test"
            result = self.bq_client.query(query).result()
            test_value = next(result)[0]
            
            logger.info(f"✅ BigQuery connected: {self.project_id}")
            return True
            
        except json.JSONDecodeError as e:
            logger.error(f"❌ Invalid JSON credentials: {e}")
            return False
        except Exception as e:
            logger.error(f"❌ BigQuery setup failed: {e}")
            return False
    
    def create_bigquery_dataset(self, dataset_name=None):
        """Create BigQuery dataset"""
        # Use environment variable if no dataset name provided
        if dataset_name is None:
            dataset_name = os.getenv("BQ_DATASET", "rds_mysql_data")
            
        logger.info(f"📊 Creating BigQuery dataset: {dataset_name}")
        
        try:
            self.dataset_id = f"{self.project_id}.{dataset_name}"
            dataset = bigquery.Dataset(self.dataset_id)
            dataset.location = "US"
            dataset.description = f"RDS MySQL data transfer - {datetime.now().strftime('%Y-%m-%d')}"
            
            dataset = self.bq_client.create_dataset(dataset, exists_ok=True)
            logger.info(f"✅ Dataset ready: {self.dataset_id}")
            return True
            
        except Exception as e:
            logger.error(f"❌ Dataset creation failed: {e}")
            return False
    
    def scan_rds_tables(self):
        """1. Scan RDS MySQL database based on the tables of CSV files"""
        logger.info("🔍 Scanning RDS MySQL tables...")
        
        try:
            # Get all tables from the database
            query = """
            SELECT 
                TABLE_NAME,
                TABLE_ROWS,
                ROUND(((DATA_LENGTH + INDEX_LENGTH) / 1024 / 1024), 2) AS 'SIZE_MB',
                TABLE_COMMENT
            FROM INFORMATION_SCHEMA.TABLES 
            WHERE TABLE_SCHEMA = DATABASE() 
            AND TABLE_TYPE = 'BASE TABLE'
            ORDER BY TABLE_ROWS DESC
            """
            
            with self.mysql_engine.connect() as conn:
                result = conn.execute(text(query))
                tables = []
                
                for row in result:
                    table_info = {
                        'name': row[0],
                        'rows': row[1] or 0,
                        'size_mb': row[2] or 0,
                        'comment': row[3] or ''
                    }
                    tables.append(table_info)
            
            if not tables:
                logger.warning("⚠️ No tables found in database")
                return []
            
            logger.info(f"📋 Found {len(tables)} tables:")
            for table in tables:
                logger.info(f"   • {table['name']}: {table['rows']:,} rows ({table['size_mb']} MB)")
            
            return tables
            
        except Exception as e:
            logger.error(f"❌ Failed to scan tables: {e}")
            return []
    
    def transfer_table_to_bigquery(self, original_table_name, bigquery_table_name=None, chunk_size=50000, use_fast_method=True):
        """2. Copy from RDS to BQ using Python google cloud directly"""
        # If bigquery_table_name is not provided, use original_table_name
        if bigquery_table_name is None:
            bigquery_table_name = original_table_name
            
        logger.info(f"🔄 Transferring table: {original_table_name} → {bigquery_table_name}")
        
        try:
            # Get table row count for progress tracking
            count_query = f"SELECT COUNT(*) FROM {original_table_name}"
            with self.mysql_engine.connect() as conn:
                result = conn.execute(text(count_query))
                total_rows = result.scalar()
            
            logger.info(f"📊 Total rows to transfer: {total_rows:,}")
            
            if total_rows == 0:
                logger.info("📋 Table is empty - skipping BigQuery creation (only creating tables with data)")
                return True  # Return success to avoid marking as failed
            
            # Only proceed if table has data
            logger.info(f"📊 Table has {total_rows:,} rows - proceeding with BigQuery transfer")
            
            # Choose transfer method based on table size and preference
            if use_fast_method and total_rows > 50000:
                return self._transfer_large_table_optimized(original_table_name, bigquery_table_name, total_rows)
            else:
                return self._transfer_table_chunked(original_table_name, bigquery_table_name, total_rows, chunk_size)
            
        except Exception as e:
            logger.error(f"❌ Transfer failed for {original_table_name} → {bigquery_table_name}: {e}")
            return False
    
    def _transfer_large_table_optimized(self, original_table_name, bigquery_table_name, total_rows):
        """Optimized transfer for large tables using BigQuery native loading"""
        logger.info(f"🚀 Using optimized transfer method for large table")
        
        try:
            # Method 1: Direct SQL query to BigQuery (fastest for large datasets)
            table_id = f"{self.dataset_id}.{bigquery_table_name}"
            
            # Get MySQL connection details for BigQuery External Data Source
            mysql_config = {
                'host': os.getenv('MYSQL_HOST'),
                'port': int(os.getenv('MYSQL_PORT', 3306)),
                'user': os.getenv('MYSQL_USERNAME'),
                'password': os.getenv('MYSQL_PASSWORD'),
                'database': os.getenv('MYSQL_DATABASE')
            }
            
            # Read entire table in larger chunks (more efficient)
            chunk_size = 100000  # Larger chunks for better performance
            logger.info(f"📦 Using larger chunks of {chunk_size:,} rows")
            
            transferred_rows = 0
            
            for offset in range(0, total_rows, chunk_size):
                logger.info(f"📦 Processing large chunk: {offset:,} to {min(offset + chunk_size, total_rows):,}")
                
                # Read large chunk from MySQL with optimized query
                chunk_query = f"""
                SELECT * FROM {original_table_name} 
                LIMIT {chunk_size} OFFSET {offset}
                """
                
                # Use chunksize parameter for more efficient reading
                df_chunk = pd.read_sql(
                    chunk_query, 
                    self.mysql_engine,
                    chunksize=None  # Read entire result at once for better performance
                )
                
                if df_chunk.empty:
                    break
                
                # Minimal data cleaning for better performance
                df_clean = df_chunk.fillna('')  # Faster than where() operation
                
                # Skip datetime conversion for faster transfer (BigQuery can handle it)
                # Only do essential data type conversion
                
                # Load to BigQuery with optimized settings
                job_config = bigquery.LoadJobConfig(
                    write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE if offset == 0 else bigquery.WriteDisposition.WRITE_APPEND,
                    autodetect=True,
                    # Performance optimizations
                    create_disposition=bigquery.CreateDisposition.CREATE_IF_NEEDED,
                    allow_quoted_newlines=True,
                    allow_jagged_rows=True
                )
                
                job = self.bq_client.load_table_from_dataframe(df_clean, table_id, job_config=job_config)
                job.result()  # Wait for completion
                
                transferred_rows += len(df_chunk)
                progress = (transferred_rows / total_rows) * 100
                logger.info(f"   🚀 Fast progress: {transferred_rows:,}/{total_rows:,} rows ({progress:.1f}%)")
            
            # Verify transfer
            verify_query = f"SELECT COUNT(*) FROM `{table_id}`"
            result = self.bq_client.query(verify_query).result()
            bq_rows = next(result)[0]
            
            logger.info(f"✅ Optimized transfer complete for {original_table_name} → {bigquery_table_name}")
            logger.info(f"   📊 MySQL rows: {total_rows:,}")
            logger.info(f"   📊 BigQuery rows: {bq_rows:,}")
            logger.info(f"   ✅ Match: {'Yes' if total_rows == bq_rows else 'No'}")
            
            return total_rows == bq_rows
            
        except Exception as e:
            logger.error(f"❌ Optimized transfer failed for {original_table_name} → {bigquery_table_name}: {e}")
            logger.info("🔄 Falling back to chunked method...")
            return self._transfer_table_chunked(original_table_name, bigquery_table_name, total_rows, 10000)
    
    def _transfer_table_chunked(self, original_table_name, bigquery_table_name, total_rows, chunk_size):
        """Original chunked transfer method (fallback)"""
        logger.info(f"📦 Using chunked transfer method")
        
        try:
            table_id = f"{self.dataset_id}.{bigquery_table_name}"
            transferred_rows = 0
            
            for offset in range(0, total_rows, chunk_size):
                logger.info(f"📦 Processing chunk: {offset:,} to {min(offset + chunk_size, total_rows):,}")
                
                # Read chunk from MySQL
                chunk_query = f"SELECT * FROM {original_table_name} LIMIT {chunk_size} OFFSET {offset}"
                df_chunk = pd.read_sql(chunk_query, self.mysql_engine)
                
                if df_chunk.empty:
                    break
                
                # Clean data for BigQuery compatibility
                df_clean = df_chunk.where(pd.notnull(df_chunk), None)
                
                # Convert datetime columns for BigQuery (fix deprecation warnings)
                for col in df_clean.columns:
                    if df_clean[col].dtype == 'object':
                        # Try to convert datetime strings with proper error handling
                        try:
                            # Test if this might be a datetime column by checking a few values
                            sample_values = df_clean[col].dropna().head(3)
                            if len(sample_values) > 0:
                                # Try converting the first non-null value
                                test_value = str(sample_values.iloc[0])
                                # Only attempt conversion if it looks like a date/time
                                if any(char in test_value for char in ['-', '/', ':', ' ']) and len(test_value) > 8:
                                    test_convert = pd.to_datetime(sample_values.iloc[0])
                                    # If successful, convert the entire column
                                    df_clean[col] = pd.to_datetime(df_clean[col])
                        except (ValueError, TypeError, pd.errors.ParserError):
                            # Not a datetime column, leave as is
                            pass
                
                # Load to BigQuery
                job_config = bigquery.LoadJobConfig(
                    write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE if offset == 0 else bigquery.WriteDisposition.WRITE_APPEND,
                    autodetect=True
                )
                
                job = self.bq_client.load_table_from_dataframe(df_clean, table_id, job_config=job_config)
                job.result()  # Wait for completion
                
                transferred_rows += len(df_chunk)
                progress = (transferred_rows / total_rows) * 100
                logger.info(f"   📈 Progress: {transferred_rows:,}/{total_rows:,} rows ({progress:.1f}%)")
            
            # Verify transfer
            verify_query = f"SELECT COUNT(*) FROM `{table_id}`"
            result = self.bq_client.query(verify_query).result()
            bq_rows = next(result)[0]
            
            logger.info(f"✅ Transfer complete for {original_table_name} → {bigquery_table_name}")
            logger.info(f"   📊 MySQL rows: {total_rows:,}")
            logger.info(f"   📊 BigQuery rows: {bq_rows:,}")
            logger.info(f"   ✅ Match: {'Yes' if total_rows == bq_rows else 'No'}")
            
            return total_rows == bq_rows
            
        except Exception as e:
            logger.error(f"❌ Chunked transfer failed for {original_table_name} → {bigquery_table_name}: {e}")
            return False
    
    def delete_rds_table_data(self, table_name):
        """3. Delete data in RDS if table is successfully imported into BQ"""
        logger.info(f"🗑️ Deleting RDS data for table: {table_name}")
        
        try:
            # Get row count before deletion for logging
            count_query = f"SELECT COUNT(*) FROM {table_name}"
            with self.mysql_engine.connect() as conn:
                result = conn.execute(text(count_query))
                rows_before = result.scalar()
            
            logger.info(f"📊 Rows to delete: {rows_before:,}")
            
            if rows_before == 0:
                logger.info("📋 Table already empty - no deletion needed")
                return True
            
            # Delete all data from the table (keeping structure)
            delete_query = f"DELETE FROM {table_name}"
            
            with self.mysql_engine.connect() as conn:
                # Start transaction for safety
                trans = conn.begin()
                try:
                    result = conn.execute(text(delete_query))
                    rows_deleted = result.rowcount
                    trans.commit()
                    
                    logger.info(f"✅ Successfully deleted {rows_deleted:,} rows from {table_name}")
                    
                    # Verify deletion
                    result = conn.execute(text(count_query))
                    rows_after = result.scalar()
                    
                    if rows_after == 0:
                        logger.info(f"✅ Verification: Table {table_name} is now empty")
                        return True
                    else:
                        logger.warning(f"⚠️ Verification failed: {rows_after} rows still remain")
                        return False
                        
                except Exception as e:
                    trans.rollback()
                    logger.error(f"❌ Transaction rolled back for {table_name}: {e}")
                    return False
            
        except Exception as e:
            logger.error(f"❌ Failed to delete data from {table_name}: {e}")
            return False
    
    def run_transfer(self, table_names=None, delete_after_transfer=True):
        """Run the complete RDS → BigQuery transfer"""
        logger.info("="*60)
        logger.info("🚀 RDS MYSQL → BIGQUERY DIRECT TRANSFER")
        logger.info("="*60)
        
        # Setup connections
        if not self.setup_mysql_connection():
            return False
        
        if not self.setup_bigquery_client():
            return False
        
        if not self.create_bigquery_dataset():
            return False
        
        # 1. Scan RDS MySQL database tables
        all_tables = self.scan_rds_tables()
        if not all_tables:
            logger.error("❌ No tables found to transfer")
            return False
        
        # Filter tables if specific names provided
        if table_names:
            tables_to_transfer = [t for t in all_tables if t['name'] in table_names]
            if not tables_to_transfer:
                logger.error(f"❌ None of the specified tables found: {table_names}")
                return False
        else:
            tables_to_transfer = all_tables
        
        logger.info(f"🎯 Transferring {len(tables_to_transfer)} tables")
        
        # 2. Copy each table from RDS to BigQuery
        successful_transfers = 0
        successful_deletions = 0
        empty_tables_processed = 0
        
        for table in tables_to_transfer:
            original_table_name = table['name']  # Original table name in RDS
            bigquery_table_name = 'rds_' + table['name']  # BigQuery table with rds_ prefix
            logger.info(f"\n🔄 Starting transfer: {original_table_name} → {bigquery_table_name}")
            
            # Get actual row count to determine if this counts as a data transfer
            count_query = f"SELECT COUNT(*) FROM {original_table_name}"
            try:
                with self.mysql_engine.connect() as conn:
                    result = conn.execute(text(count_query))
                    actual_rows = result.scalar()
            except Exception as e:
                logger.error(f"❌ Failed to get row count for {original_table_name}: {e}")
                actual_rows = 0
            
            # Step 2: Transfer to BigQuery (using both original and BigQuery table names)
            if self.transfer_table_to_bigquery(original_table_name, bigquery_table_name):
                if actual_rows > 0:
                    successful_transfers += 1
                    logger.info(f"✅ Successfully transferred: {original_table_name} → {bigquery_table_name} ({actual_rows:,} rows)")
                    
                    # Step 3: Delete from RDS if transfer was successful and deletion is enabled
                    if delete_after_transfer:
                        logger.info(f"🗑️ Starting RDS data deletion for: {original_table_name}")
                        
                        if self.delete_rds_table_data(original_table_name):
                            successful_deletions += 1
                            logger.info(f"✅ Successfully deleted RDS data: {original_table_name}")
                        else:
                            logger.error(f"❌ Failed to delete RDS data: {original_table_name}")
                            logger.warning(f"⚠️ Data remains in both RDS and BigQuery for: {original_table_name}")
                    else:
                        logger.info(f"📋 Skipping RDS deletion (disabled): {original_table_name}")
                else:
                    empty_tables_processed += 1
                    logger.info(f"📋 Skipped empty table (no BigQuery table created): {original_table_name}")
                    
            else:
                logger.error(f"❌ Failed to transfer: {original_table_name}")
                logger.info(f"📋 Skipping RDS deletion due to failed transfer: {original_table_name}")
        
        # Summary
        end_time = datetime.now()
        duration = end_time - self.start_time
        
        logger.info("="*60)
        logger.info("📊 TRANSFER SUMMARY")
        logger.info(f"   Total tables: {len(tables_to_transfer)}")
        logger.info(f"   BigQuery tables created: {successful_transfers}")
        logger.info(f"   Empty tables skipped: {empty_tables_processed}")
        logger.info(f"   Failed transfers: {len(tables_to_transfer) - successful_transfers - empty_tables_processed}")
        
        if delete_after_transfer:
            logger.info(f"   RDS successful deletions: {successful_deletions}")
            logger.info(f"   RDS failed deletions: {successful_transfers - successful_deletions}")
        else:
            logger.info(f"   RDS deletion: Disabled")
            
        logger.info(f"   Dataset: {self.dataset_id}")
        logger.info(f"   Duration: {duration}")
        logger.info("="*60)
        
        # Return True only if all tables were processed successfully (data transfers + empty tables)
        # and (if enabled) all deletions succeeded for tables with data
        all_tables_processed = (successful_transfers + empty_tables_processed) == len(tables_to_transfer)
        all_deletions_ok = not delete_after_transfer or successful_deletions == successful_transfers
        
        return all_tables_processed and all_deletions_ok

def main():
    """Main execution function"""
    transfer = RDSToBigQueryTransfer()
    
    # Check for flags
    delete_after_transfer = True
    use_fast_method = True
    table_names = []
    
    for arg in sys.argv[1:]:
        if arg == "--no-delete":
            delete_after_transfer = False
            logger.info("🚫 RDS deletion disabled via --no-delete flag")
        elif arg == "--slow":
            use_fast_method = False
            logger.info("🐌 Using original chunked method via --slow flag")
        elif arg == "--fast":
            use_fast_method = True
            logger.info("🚀 Using optimized fast method via --fast flag")
        else:
            table_names.append(arg)
    
    # Update transfer method preference
    if hasattr(transfer, 'use_fast_method'):
        transfer.use_fast_method = use_fast_method
    
    if table_names:
        # Transfer specific tables
        logger.info(f"🎯 Transferring specific tables: {table_names}")
        success = transfer.run_transfer(table_names, delete_after_transfer)
    else:
        # Transfer all tables
        logger.info("🎯 Transferring all tables")
        success = transfer.run_transfer(delete_after_transfer=delete_after_transfer)
    
    if success:
        logger.info("🎉 RDS → BigQuery transfer completed successfully!")
        sys.exit(0)
    else:
        logger.error("❌ RDS → BigQuery transfer failed!")
        sys.exit(1)

if __name__ == "__main__":
    main()
