#!/usr/bin/env python3
"""
RDS Data Cleanup Script for Meltano Pipeline
Deletes (truncates) data from RDS MySQL tables after successful BigQuery imports

This script is designed to run after Meltano ETL pipeline to clean up source data.
It verifies data existence in BigQuery before truncating corresponding RDS tables.

Usage:
    python delete-rds-after-load.py                    # Check and delete all tables
    python delete-rds-after-load.py --table table_name # Delete specific table
    python delete-rds-after-load.py --dry-run          # Show what would be deleted
    python delete-rds-after-load.py --verify-only      # Only verify BQ data, no deletion
    python delete-rds-after-load.py --force            # Delete without verification
"""

import os
import sys
import json
import logging
import argparse
from datetime import datetime
from typing import List, Dict, Any, Optional, Tuple
from dotenv import load_dotenv

# Database imports
import pymysql
from sqlalchemy import create_engine, text
from google.cloud import bigquery
from google.oauth2 import service_account

# Load environment variables
load_dotenv('../.env')

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('delete-rds-after-load.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class RDSCleanupManager:
    """Manages RDS data cleanup after successful BigQuery imports"""
    
    def __init__(self):
        self.start_time = datetime.now()
        self.mysql_engine = None
        self.bq_client = None
        self.project_id = None
        self.dataset_id = None
        logger.info(f"🧹 Starting RDS Cleanup Manager at {self.start_time}")
    
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
                logger.error(f" Missing MySQL config: {missing}")
                return False
            
            # Create SQLAlchemy engine
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
            logger.error(f" MySQL connection failed: {e}")
            return False
    
    def setup_bigquery_client(self):
        """Setup BigQuery client"""
        logger.info("🔗 Setting up BigQuery client...")
        
        try:
            credentials_json = os.getenv('GOOGLE_APPLICATION_CREDENTIALS_JSON')
            if not credentials_json:
                logger.error(" GOOGLE_APPLICATION_CREDENTIALS_JSON not set")
                return False
            
            credentials_dict = json.loads(credentials_json)
            credentials = service_account.Credentials.from_service_account_info(credentials_dict)
            
            self.project_id = credentials_dict.get('project_id')
            if not self.project_id:
                logger.error(" No project_id in credentials")
                return False
            
            self.bq_client = bigquery.Client(project=self.project_id, credentials=credentials)
            
            # Set dataset ID (could be from env or default from meltano.yml)
            self.dataset_id = f"{self.project_id}.{os.getenv('BQ_DATASET', 'bec_bq')}"
            
            # Test connection
            query = "SELECT 1 as test"
            result = self.bq_client.query(query).result()
            test_value = next(result)[0]
            
            logger.info(f" BigQuery connected: {self.project_id}")
            logger.info(f" Target dataset: {self.dataset_id}")
            return True
            
        except json.JSONDecodeError as e:
            logger.error(f" Invalid JSON credentials: {e}")
            return False
        except Exception as e:
            logger.error(f" BigQuery setup failed: {e}")
            return False
    
    def get_rds_tables(self) -> List[Dict[str, Any]]:
        """Get list of tables from RDS with row counts"""
        logger.info("🔍 Scanning RDS MySQL tables...")
        
        try:
            query = """
            SELECT 
                TABLE_NAME as table_name,
                TABLE_ROWS as estimated_rows,
                ROUND(((DATA_LENGTH + INDEX_LENGTH) / 1024 / 1024), 2) AS size_mb,
                TABLE_COMMENT as comment
            FROM INFORMATION_SCHEMA.TABLES 
            WHERE TABLE_SCHEMA = DATABASE() 
            AND TABLE_TYPE = 'BASE TABLE'
            ORDER BY TABLE_ROWS DESC
            """
            
            with self.mysql_engine.connect() as conn:
                result = conn.execute(text(query))
                tables = []
                
                for row in result:
                    # Get actual row count
                    count_query = f"SELECT COUNT(*) FROM `{row.table_name}`"
                    count_result = conn.execute(text(count_query))
                    actual_rows = count_result.scalar()
                    
                    table_info = {
                        'name': row.table_name,
                        'estimated_rows': row.estimated_rows or 0,
                        'actual_rows': actual_rows,
                        'size_mb': row.size_mb or 0,
                        'comment': row.comment or ''
                    }
                    tables.append(table_info)
            
            logger.info(f"📋 Found {len(tables)} tables in RDS:")
            for table in tables:
                logger.info(f"   • {table['name']}: {table['actual_rows']:,} rows ({table['size_mb']} MB)")
            
            return tables
            
        except Exception as e:
            logger.error(f" Failed to scan RDS tables: {e}")
            return []
    
    def verify_bigquery_data(self, table_name: str) -> Tuple[bool, int]:
        """Verify data exists in BigQuery for a given table"""
        logger.info(f"🔍 Verifying BigQuery data for table: {table_name}")
        
        try:
            # Check if table exists in BigQuery
            table_ref = f"{self.dataset_id}.{table_name}"
            
            # Get row count from BigQuery
            query = f"SELECT COUNT(*) as row_count FROM `{table_ref}`"
            result = self.bq_client.query(query).result()
            bq_rows = next(result)[0]
            
            if bq_rows > 0:
                logger.info(f" BigQuery table {table_name}: {bq_rows:,} rows found")
                return True, bq_rows
            else:
                logger.warning(f" BigQuery table {table_name}: No data found")
                return False, 0
                
        except Exception as e:
            logger.error(f" BigQuery verification failed for {table_name}: {e}")
            return False, 0
    
    def truncate_rds_table(self, table_name: str, dry_run: bool = False) -> bool:
        """Truncate (delete all data) from RDS table"""
        logger.info(f"🗑️ {'[DRY RUN] ' if dry_run else ''}Truncating RDS table: {table_name}")
        
        if dry_run:
            logger.info(f"🧪 DRY RUN: Would execute TRUNCATE TABLE `{table_name}`")
            return True
        
        try:
            with self.mysql_engine.connect() as conn:
                # Get row count before truncation
                count_query = f"SELECT COUNT(*) FROM `{table_name}`"
                result = conn.execute(text(count_query))
                rows_before = result.scalar()
                
                if rows_before == 0:
                    logger.info(f" Table {table_name} is already empty")
                    return True
                
                logger.info(f" Rows to delete: {rows_before:,}")
                
                # Use TRUNCATE for faster deletion (resets auto-increment)
                # No explicit transaction needed as TRUNCATE is atomic
                truncate_query = f"TRUNCATE TABLE `{table_name}`"
                conn.execute(text(truncate_query))
                conn.commit()  # Ensure the operation is committed
                
                # Verify truncation
                result = conn.execute(text(count_query))
                rows_after = result.scalar()
                
                if rows_after == 0:
                    logger.info(f" Successfully truncated {table_name} ({rows_before:,} rows deleted)")
                    return True
                else:
                    logger.error(f" Truncation verification failed: {rows_after} rows remain")
                    return False
                    
        except Exception as e:
            logger.error(f" Failed to truncate {table_name}: {e}")
            return False
    
    def cleanup_tables(self, specific_table: Optional[str] = None, 
                      dry_run: bool = False, verify_only: bool = False, 
                      force: bool = False) -> bool:
        """Main cleanup process"""
        logger.info("="*60)
        logger.info("🧹 RDS CLEANUP AFTER BIGQUERY IMPORT")
        logger.info("="*60)
        
        # Setup connections
        if not self.setup_mysql_connection():
            return False
        
        if not self.setup_bigquery_client():
            return False
        
        # Get RDS tables
        rds_tables = self.get_rds_tables()
        if not rds_tables:
            logger.error(" No tables found in RDS")
            return False
        
        # Filter tables if specific table requested
        if specific_table:
            rds_tables = [t for t in rds_tables if t['name'] == specific_table]
            if not rds_tables:
                logger.error(f" Table '{specific_table}' not found in RDS")
                return False
        
        logger.info(f"🎯 Processing {len(rds_tables)} table(s)")
        
        # Process each table
        verified_tables = 0
        cleaned_tables = 0
        skipped_tables = 0
        
        for table_info in rds_tables:
            table_name = table_info['name']
            rds_rows = table_info['actual_rows']
            
            logger.info(f"\n Processing table: {table_name}")
            logger.info(f" RDS rows: {rds_rows:,}")
            
            if rds_rows == 0:
                logger.info(f"📋 Table {table_name} is already empty, skipping")
                skipped_tables += 1
                continue
            
            # Verify BigQuery data (unless force mode)
            if not force:
                bq_verified, bq_rows = self.verify_bigquery_data(table_name)
                
                if not bq_verified:
                    logger.warning(f"  Skipping {table_name}: No data found in BigQuery")
                    skipped_tables += 1
                    continue
                
                logger.info(f"📊 BigQuery rows: {bq_rows:,}")
                
                # Additional verification: row count comparison
                if bq_rows < rds_rows * 0.9:  # Allow 10% tolerance
                    logger.warning(f"  Row count mismatch for {table_name}:")
                    logger.warning(f"   RDS: {rds_rows:,}, BigQuery: {bq_rows:,}")
                    logger.warning(f"   Skipping cleanup (use --force to override)")
                    skipped_tables += 1
                    continue
                
                verified_tables += 1
            else:
                logger.info("FORCE MODE: Skipping BigQuery verification")
                verified_tables += 1
            
            if verify_only:
                logger.info(f"Verification passed for {table_name}")
                continue
            
            # Perform cleanup
            success = self.truncate_rds_table(table_name, dry_run)
            
            if success:
                cleaned_tables += 1
                logger.info(f"{'[DRY RUN] ' if dry_run else ''}Cleanup successful: {table_name}")
            else:
                logger.error(f"Cleanup failed: {table_name}")
        
        # Summary
        end_time = datetime.now()
        duration = end_time - self.start_time
        
        logger.info("="*60)
        logger.info("   CLEANUP SUMMARY")
        logger.info(f"   Total tables: {len(rds_tables)}")
        logger.info(f"   Verified in BigQuery: {verified_tables}")
        logger.info(f"   {'[DRY RUN] ' if dry_run else ''}Cleaned tables: {cleaned_tables}")
        logger.info(f"   Skipped tables: {skipped_tables}")
        logger.info(f"   Duration: {duration}")
        
        if verify_only:
            logger.info(f"   Mode: Verification only")
        elif dry_run:
            logger.info(f"   Mode: Dry run")
        elif force:
            logger.info(f"   Mode: Force cleanup")
        else:
            logger.info(f"   Mode: Standard cleanup")
            
        logger.info("="*60)
        
        # Return success condition based on mode
        if verify_only:
            # In verification-only mode, success means we could verify the data
            return verified_tables > 0
        else:
            # In cleanup mode, success means we cleaned all verified tables
            return cleaned_tables == verified_tables

def main():
    """Main execution function"""
    parser = argparse.ArgumentParser(description="RDS Data Cleanup after BigQuery Import")
    parser.add_argument('--table', help="Specific table to clean up")
    parser.add_argument('--dry-run', action='store_true', 
                       help="Show what would be deleted without actually deleting")
    parser.add_argument('--verify-only', action='store_true',
                       help="Only verify BigQuery data, don't delete anything")
    parser.add_argument('--force', action='store_true',
                       help="Delete without BigQuery verification (DANGEROUS)")
    
    args = parser.parse_args()
    
    if args.force and (args.dry_run or args.verify_only):
        logger.error("--force cannot be used with --dry-run or --verify-only")
        sys.exit(1)
    
    cleanup_manager = RDSCleanupManager()
    
    success = cleanup_manager.cleanup_tables(
        specific_table=args.table,
        dry_run=args.dry_run,
        verify_only=args.verify_only,
        force=args.force
    )
    
    if success:
        logger.info("RDS cleanup completed successfully!")
        sys.exit(0)
    else:
        logger.error("RDS cleanup failed!")
        sys.exit(1)

if __name__ == "__main__":
    main()