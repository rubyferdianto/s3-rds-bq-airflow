#!/usr/bin/env python3
"""
RDS MySQL to BigQuery Pipeline - Direct Transfer
Extracts data from AWS RDS MySQL and loads into Google BigQuery using direct Python libraries
"""

import os
import sys
import json
import pandas as pd
import pymysql
from google.cloud import bigquery
from google.cloud.bigquery import LoadJobConfig, WriteDisposition
from dotenv import load_dotenv
import logging

# Load environment variables
# Try to load from parent directory first (when called from main.py), then current directory
if os.path.exists('../.env'):
    load_dotenv('../.env')
elif os.path.exists('.env'):
    load_dotenv('.env')
else:
    load_dotenv()  # Use default behavior

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def check_environment():
    """Check if required environment variables are set"""
    required_vars = [
        'MYSQL_HOST', 'MYSQL_USERNAME', 'MYSQL_PASSWORD', 'MYSQL_DATABASE', 'MYSQL_PORT',
        'GCP_PROJECT', 'BQ_DATASET', 'BQ_LOCATION', 'GOOGLE_APPLICATION_CREDENTIALS_JSON'
    ]
    
    missing_vars = [var for var in required_vars if not os.getenv(var)]
    
    if missing_vars:
        logger.error(f"❌ Missing required environment variables: {', '.join(missing_vars)}")
        return False
    
    logger.info("✅ Environment variables check passed")
    return True

def setup_mysql_connection():
    """Setup MySQL connection"""
    connection_config = {
        'host': os.getenv('MYSQL_HOST'),
        'port': int(os.getenv('MYSQL_PORT', 3306)),
        'user': os.getenv('MYSQL_USERNAME'),
        'password': os.getenv('MYSQL_PASSWORD'),
        'database': os.getenv('MYSQL_DATABASE'),
        'charset': 'utf8mb4'
    }
    
    try:
        connection = pymysql.connect(**connection_config)
        logger.info(f"✅ Connected to MySQL: {connection_config['host']}")
        return connection
    except Exception as e:
        logger.error(f"❌ MySQL connection failed: {str(e)}")
        return None

def setup_bigquery_client():
    """Setup BigQuery client"""
    credentials_json = os.getenv('GOOGLE_APPLICATION_CREDENTIALS_JSON')
    if not credentials_json:
        logger.error("❌ GOOGLE_APPLICATION_CREDENTIALS_JSON not found")
        return None
    
    try:
        credentials_dict = json.loads(credentials_json)
        client = bigquery.Client.from_service_account_info(
            credentials_dict,
            project=credentials_dict['project_id']
        )
        logger.info(f"✅ BigQuery client created for project: {credentials_dict['project_id']}")
        return client
    except Exception as e:
        logger.error(f"❌ BigQuery client setup failed: {str(e)}")
        return None

def get_mysql_tables(connection):
    """Get list of tables from MySQL database"""
    try:
        with connection.cursor() as cursor:
            cursor.execute("SHOW TABLES")
            tables = [row[0] for row in cursor.fetchall()]
        logger.info(f"✅ Found {len(tables)} tables in MySQL")
        return tables
    except Exception as e:
        logger.error(f"❌ Error getting MySQL tables: {str(e)}")
        return []

def cleanup_rds_table(mysql_conn, table_name):
    """Delete data from RDS table after successful BigQuery transfer"""
    # Check if cleanup is enabled (default: False for safety)
    cleanup_enabled = os.getenv('RDS_CLEANUP_AFTER_TRANSFER', 'false').lower() in ['true', '1', 'yes']
    
    if not cleanup_enabled:
        logger.info(f"   ℹ️ RDS cleanup disabled for {table_name} (set RDS_CLEANUP_AFTER_TRANSFER=true to enable)")
        return True
    
    try:
        cursor = mysql_conn.cursor()
        
        # Get row count before deletion for logging
        cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
        row_count = cursor.fetchone()[0]
        
        if row_count == 0:
            logger.info(f"   ℹ️ Table {table_name} is already empty, no cleanup needed")
            return True
        
        # Delete all data from the table (but keep the table structure)
        logger.info(f"   🗑️ Cleaning up {row_count:,} rows from RDS table {table_name}...")
        cursor.execute(f"DELETE FROM {table_name}")
        mysql_conn.commit()
        
        # Verify deletion
        cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
        remaining_rows = cursor.fetchone()[0]
        
        if remaining_rows == 0:
            logger.info(f"   ✅ Successfully cleaned up RDS table {table_name}")
            return True
        else:
            logger.warning(f"   ⚠️ Cleanup incomplete: {remaining_rows} rows remaining in {table_name}")
            return False
            
    except Exception as e:
        logger.error(f"   ❌ Error cleaning up RDS table {table_name}: {str(e)}")
        return False

def transfer_table(mysql_conn, bq_client, table_name):
    """Transfer a single table from MySQL to BigQuery"""
    logger.info(f"🚀 Transferring table: {table_name}")
    
    try:
        # Read data from MySQL
        query = f"SELECT * FROM {table_name}"
        df = pd.read_sql(query, mysql_conn)
        
        if df.empty:
            logger.info(f"   ℹ️ Table {table_name} is empty, no data to transfer")
            # Empty table is not a failure - it's a successful "no-op" operation
            return True
        
        logger.info(f"   📊 Found {len(df)} rows, {len(df.columns)} columns")
        
        # Prepare BigQuery dataset and table references
        project_id = os.getenv('GCP_PROJECT')
        dataset_id = os.getenv('BQ_DATASET')
        
        dataset_ref = bq_client.dataset(dataset_id)
        table_ref = dataset_ref.table(table_name)
        
        # Configure load job
        job_config = LoadJobConfig()
        job_config.write_disposition = WriteDisposition.WRITE_TRUNCATE
        job_config.autodetect = True
        
        # Load data to BigQuery
        job = bq_client.load_table_from_dataframe(df, table_ref, job_config=job_config)
        job.result()  # Wait for job to complete
        
        # Verify the load
        table = bq_client.get_table(table_ref)
        logger.info(f"   ✅ Loaded {table.num_rows:,} rows to BigQuery table {table_name}")
        
        # Clean up RDS data after successful BigQuery transfer
        cleanup_success = cleanup_rds_table(mysql_conn, table_name)
        if cleanup_success:
            logger.info(f"   🧹 RDS cleanup completed for {table_name}")
        else:
            logger.warning(f"   ⚠️ RDS cleanup failed for {table_name} (BigQuery transfer still successful)")
        
        return True
        
    except Exception as e:
        logger.error(f"   ❌ Error transferring {table_name}: {str(e)}")
        return False

def main():
    """Main pipeline function"""
    logger.info("=" * 70)
    logger.info("🎯 STARTING RDS MYSQL TO BIGQUERY PIPELINE")
    logger.info("=" * 70)
    
    # Check environment
    if not check_environment():
        sys.exit(1)
    
    # Display configuration
    logger.info("📋 Pipeline Configuration:")
    logger.info(f"  MySQL Host: {os.getenv('MYSQL_HOST')}")
    logger.info(f"  MySQL Database: {os.getenv('MYSQL_DATABASE')}")
    logger.info(f"  GCP Project: {os.getenv('GCP_PROJECT')}")
    logger.info(f"  BigQuery Dataset: {os.getenv('BQ_DATASET')}")
    logger.info(f"  BigQuery Location: {os.getenv('BQ_LOCATION')}")
    
    # Check cleanup setting
    cleanup_enabled = os.getenv('RDS_CLEANUP_AFTER_TRANSFER', 'false').lower() in ['true', '1', 'yes']
    logger.info(f"  RDS Cleanup After Transfer: {'✅ ENABLED' if cleanup_enabled else '❌ DISABLED'}")
    
    if not cleanup_enabled:
        logger.info("  💡 To enable RDS cleanup: set RDS_CLEANUP_AFTER_TRANSFER=true")
    else:
        logger.warning("  ⚠️ RDS data will be DELETED after successful BigQuery transfer!")
    
    # Setup connections
    mysql_conn = setup_mysql_connection()
    if not mysql_conn:
        sys.exit(1)
    
    bq_client = setup_bigquery_client()
    if not bq_client:
        mysql_conn.close()
        sys.exit(1)
    
    try:
        # Get list of tables
        logger.info("=" * 70)
        logger.info("STEP 1: DISCOVERING MYSQL TABLES")
        logger.info("=" * 70)
        
        tables = get_mysql_tables(mysql_conn)
        if not tables:
            logger.warning("⚠️ No tables found in database")
            logger.info("=" * 70)
            logger.info("🎉 PIPELINE COMPLETED SUCCESSFULLY!")
            logger.info("=" * 70)
            logger.info("💡 Database exists but contains no tables - this is not an error")
            logger.info("ℹ️ Pipeline completed successfully with no data to transfer")
            return
        
        # Transfer each table
        cleanup_enabled = os.getenv('RDS_CLEANUP_AFTER_TRANSFER', 'false').lower() in ['true', '1', 'yes']
        step_description = "STEP 2: TRANSFERRING DATA TO BIGQUERY"
        if cleanup_enabled:
            step_description += " WITH RDS CLEANUP"
        
        logger.info("=" * 70)
        logger.info(step_description)
        logger.info("=" * 70)
        
        successful_transfers = 0
        failed_transfers = 0
        cleanup_summary = []
        
        for table_name in tables:
            logger.info(f"🔄 Processing table: {table_name}")
            success = transfer_table(mysql_conn, bq_client, table_name)
            if success:
                successful_transfers += 1
                # Check if table actually had data to determine the summary message
                try:
                    cursor = mysql_conn.cursor()
                    cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
                    row_count = cursor.fetchone()[0]
                    if row_count > 0:
                        cleanup_summary.append(f"✅ {table_name}: {row_count:,} rows transferred")
                    else:
                        cleanup_summary.append(f"ℹ️ {table_name}: Empty table (no data to transfer)")
                except:
                    cleanup_summary.append(f"✅ {table_name}: Transfer completed")
            else:
                failed_transfers += 1
                cleanup_summary.append(f"❌ {table_name}: Transfer failed")
        
        # Summary
        logger.info("=" * 70)
        logger.info("📋 PIPELINE SUMMARY")
        logger.info("=" * 70)
        logger.info(f"   ✅ Successful transfers: {successful_transfers}")
        logger.info(f"   ❌ Failed transfers: {failed_transfers}")
        logger.info(f"   📊 Total tables: {len(tables)}")
        
        # Cleanup summary
        logger.info("\n🧹 RDS CLEANUP SUMMARY:")
        for summary_line in cleanup_summary:
            logger.info(f"   {summary_line}")
        
        # Determine success based on whether there were actual errors, not just empty tables
        if failed_transfers > 0:
            logger.error("=" * 70)
            logger.error("❌ PIPELINE COMPLETED WITH ERRORS")
            logger.error("=" * 70)
            logger.error(f"⚠️ {failed_transfers} table(s) failed to transfer due to errors")
            sys.exit(1)
        else:
            project_id = os.getenv('GCP_PROJECT')
            dataset_id = os.getenv('BQ_DATASET')
            cleanup_enabled = os.getenv('RDS_CLEANUP_AFTER_TRANSFER', 'false').lower() in ['true', '1', 'yes']
            
            logger.info("=" * 70)
            logger.info("🎉 PIPELINE COMPLETED SUCCESSFULLY!")
            logger.info("=" * 70)
            
            if successful_transfers > 0:
                logger.info("💡 Your RDS MySQL data has been loaded into BigQuery!")
                
                if cleanup_enabled:
                    logger.info("🧹 RDS tables have been cleaned up after successful transfer!")
                else:
                    logger.info("💾 RDS data retained (cleanup disabled)")
                    
                logger.info(f"🔍 Check your BigQuery dataset: {project_id}.{dataset_id}")
            else:
                logger.info("ℹ️ All tables were empty - no data to transfer")
                logger.info("💡 This is not an error condition - pipeline completed successfully")
        
    finally:
        mysql_conn.close()
        logger.info("✅ MySQL connection closed")

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        logger.info("\n🛑 Pipeline interrupted by user")
        sys.exit(130)
    except Exception as e:
        logger.error(f"❌ Unexpected error: {str(e)}")
        sys.exit(1)
