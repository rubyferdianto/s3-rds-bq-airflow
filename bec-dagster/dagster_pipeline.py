"""
Professional data orchestration tool for automated data pipeline management
"""

import os
import glob
import subprocess
from pathlib import Path
from typing import List, Dict, Any
from dotenv import load_dotenv
import boto3
from botocore.exceptions import ClientError, NoCredentialsError
import psycopg2

# Load environment variables from .env file in parent directory
load_dotenv('../.env')

from dagster import (
    asset, 
    job, 
    materialize,
    AssetMaterialization,
    AssetObservation,
    Output,
    MetadataValue,
    Config,
    get_dagster_logger,
    Definitions
)


class PipelineConfig(Config):
    """Configuration for the pipeline"""
    csv_directory: str = "../bec-aws-bq/csv-source-file"  # Updated to correct path from DAGSTER folder
    s3_bucket: str = os.getenv("S3_BUCKET", "bec-bucket-aws")
    s3_source_prefix: str = os.getenv("S3_SOURCE_PREFIX", "s3-to-rds")
    s3_imported_prefix: str = os.getenv("S3_IMPORTED_PREFIX", "s3-imported-to-rds")
    rds_host: str = os.getenv("MYSQL_HOST", "bec-database.c123456789.us-east-1.rds.amazonaws.com")
    bigquery_dataset: str = os.getenv("BQ_DATASET", "bec_dataset")

 
@asset(group_name="Extraction")
def _1_s3_to_rds(config: PipelineConfig) -> List[str]:
    """
    Move existing CSV files from S3 and into RDS tables
    This function directly lists files from S3 bucket without depending on local CSV listing

    Returns:
        Processed CSV from S3 Bucket to AWS RDS
    """
    logger = get_dagster_logger()

    logger.info("🔄 Extraction - Upload S3 to RDS...")

    # Initialize S3 client
    try:
        s3_client = boto3.client('s3')
        logger.info(f"Connected to S3 service")
    except NoCredentialsError:
        logger.error("AWS credentials not found. Please configure AWS credentials.")
        raise
    except Exception as e:
        logger.error(f"Failed to initialize S3 client: {str(e)}")
        raise
    
    # List CSV files directly from S3 bucket
    logger.info(f"🔍 Listing CSV files from S3 bucket: {config.s3_bucket}")
    logger.info(f"🔍 Looking in prefix: {config.s3_source_prefix}")
    
    s3_paths = []
    try:
        # List objects in the S3 bucket with the specified prefix
        response = s3_client.list_objects_v2(
            Bucket=config.s3_bucket,
            Prefix=f"{config.s3_source_prefix}"
        )
        
        if 'Contents' in response:
            for obj in response['Contents']:
                key = obj['Key']
                # Only include CSV files
                if key.endswith('.csv'):
                    s3_path = f"s3://{config.s3_bucket}/{key}"
                    s3_paths.append(s3_path)
                    logger.info(f"Found S3 CSV file: {s3_path}")
            
            logger.info(f"✅ Found {len(s3_paths)} CSV files in S3")
        else:
            logger.warning(f"No files found in S3 bucket {config.s3_bucket} with prefix {config.s3_source_prefix}")
            
    except ClientError as e:
        error_code = e.response['Error']['Code']
        if error_code == 'NoSuchBucket':
            logger.error(f"S3 bucket '{config.s3_bucket}' does not exist")
        elif error_code == 'AccessDenied':
            logger.error(f"Access denied to S3 bucket '{config.s3_bucket}'")
        else:
            logger.error(f"S3 error: {e}")
        raise
    except Exception as e:
        logger.error(f"Error listing S3 files: {str(e)}")
        raise
    
    if not s3_paths:
        logger.warning("No CSV files found in S3. Pipeline will continue but RDS import may fail.")
        return s3_paths
    
    # Execute S3 to RDS import
    logger.info("🔄 Importing S3 files to RDS...")
    try:
        # Use conda environment to ensure all dependencies are available
        rds_result = subprocess.run([
            'bash', '-c', 
            'eval "$(conda shell.bash hook)" && conda activate bec && python main.py --stage s3-rds'
        ], 
            capture_output=True,
            text=True,
            cwd=".."  # Run from parent directory
        )
        
        if rds_result.returncode != 0:
            logger.error(f"S3 to RDS import failed with exit code {rds_result.returncode}")
            logger.error(f"Error details: {rds_result.stderr}")
            logger.error(f"Output: {rds_result.stdout}")
            raise Exception(f"RDS import failed with exit code {rds_result.returncode}. Error: {rds_result.stderr}")
        
        logger.info("✅ S3 to RDS import completed successfully")
        logger.info(f"RDS import output: {rds_result.stdout}")
        
        # Verify that files were moved to imported folder
        logger.info("🔍 Verifying files were moved to imported folder...")
        try:
            imported_response = s3_client.list_objects_v2(
                Bucket=config.s3_bucket,
                Prefix=f"{config.s3_imported_prefix}"
            )
            
            imported_files = []
            if 'Contents' in imported_response:
                for obj in imported_response['Contents']:
                    key = obj['Key']
                    if key.endswith('.csv'):
                        imported_files.append(key)
                        logger.info(f"✅ Found imported file: {key}")
            
            logger.info(f"📊 Files successfully moved to imported folder: {len(imported_files)}")
            
            # Check if source files still exist (they should be deleted)
            source_response = s3_client.list_objects_v2(
                Bucket=config.s3_bucket,
                Prefix=f"{config.s3_source_prefix}"
            )
            
            remaining_files = []
            if 'Contents' in source_response:
                for obj in source_response['Contents']:
                    key = obj['Key']
                    if key.endswith('.csv'):
                        remaining_files.append(key)
                        logger.info(f"⚠️ File still in source folder: {key}")
            
            if remaining_files:
                logger.warning(f"⚠️ {len(remaining_files)} files still remain in source folder")
            else:
                logger.info("✅ All source files successfully cleared")
                
        except Exception as e:
            logger.warning(f"⚠️ Could not verify file movement: {str(e)}")
        
        # Generate table names for imported data
        table_names = []
        for s3_path in s3_paths:
            filename = os.path.basename(s3_path).replace('.csv', '')
            table_name = filename.lower().replace('-', '_')
            table_names.append(table_name)
        
        logger.info(f"Tables created in RDS: {len(table_names)}")
        logger.info(f"RDS host: {config.rds_host}")
        
    except Exception as e:
        logger.error(f"Error during RDS import: {str(e)}")
        raise
    
    logger.info("🎉 S3 to RDS operation completed successfully!")
    logger.info(f"S3 files processed: {len(s3_paths)} → RDS tables: {len(table_names)}")

    return s3_paths


@asset(group_name="Loading", deps=[_1_s3_to_rds])
def _2_staging_to_bigquery(config: PipelineConfig, _1_s3_to_rds) -> Dict[str, Any]:
    """
    Loading data from both Supabase and AWS RDS MySQL into Google Cloud BigQuery
    
    This function processes both data sources in sequence:
    1. First: Supabase tables
    2. Second: AWS RDS MySQL tables

    Args:
        _1_s3_to_rds: Result from previous stage (may be None if failed)

    Returns:
        Combined results from both Supabase and RDS to BigQuery transfers
    """
    logger = get_dagster_logger()

    logger.info("🔄 Loading - Dual Database (Supabase + RDS) to BigQuery...")
    logger.info("💡 Processing both Supabase and RDS data sources sequentially")
    
    # Initialize collections for both data sources
    all_table_names = []
    all_bq_tables = []
    all_transfer_logs = []
    s3_paths_for_metadata = []
    
    # Get S3 metadata if available from previous stage
    if _1_s3_to_rds and isinstance(_1_s3_to_rds, list) and len(_1_s3_to_rds) > 0:
        logger.info("✅ Using S3 metadata from successful _1_s3_to_rds stage")
        for s3_path in _1_s3_to_rds:
            s3_paths_for_metadata.append(s3_path)
        logger.info(f"📊 Got {len(s3_paths_for_metadata)} S3 files for metadata")
    
    # ===========================================
    # PHASE 1: Process Supabase tables
    # ===========================================
    logger.info("🚀 PHASE 1: Processing Supabase tables...")
    supabase_tables = []
    
    try:
        # Use PostgreSQL connection (same as Meltano) instead of Supabase REST API
        import psycopg2
        
        # Get PostgreSQL connection details for Supabase
        supabase_host = "aws-1-ap-southeast-1.pooler.supabase.com"
        supabase_port = 5432
        supabase_database = "postgres"
        supabase_user = "postgres.royhmnxmsfichopabwsi"
        supabase_password = os.getenv("TAP_POSTGRES_PASSWORD", "MD4mq0O6AA4qlfpt")
        
        if supabase_password:
            logger.info("✅ Connected to Supabase via PostgreSQL")
            
            # Connect to Supabase PostgreSQL database
            conn = psycopg2.connect(
                host=supabase_host,
                port=supabase_port,
                database=supabase_database,
                user=supabase_user,
                password=supabase_password
            )
            
            cursor = conn.cursor()
            
            # Get table list using PostgreSQL query
            cursor.execute("""
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema = 'public' 
                AND table_type = 'BASE TABLE'
                AND table_name LIKE '%olist%' OR table_name LIKE '%product_category%'
                ORDER BY table_name;
            """)
            
            supabase_tables = [row[0] for row in cursor.fetchall()]
            cursor.close()
            conn.close()
            
            if supabase_tables:
                logger.info(f"📊 Discovered {len(supabase_tables)} tables from Supabase via PostgreSQL: {supabase_tables}")
            else:
                logger.info("No tables found in Supabase PostgreSQL")
                
        else:
            logger.warning("⚠️ TAP_POSTGRES_PASSWORD not found in environment variables")
            logger.info("💡 Set TAP_POSTGRES_PASSWORD to enable Supabase processing")
            
    except ImportError as import_error:
        logger.warning(f"⚠️ PostgreSQL client not available: {import_error}")
        logger.info("💡 Install with: pip install psycopg2-binary")
    except Exception as supabase_error:
        logger.error(f"❌ Could not connect to Supabase: {str(supabase_error)}")
    
    # Process Supabase tables if found
    if supabase_tables:
        logger.info(f"🔄 Processing {len(supabase_tables)} Supabase tables for BigQuery transfer...")
        
        # Create detailed log file for Supabase transfer
        supabase_log_file = "../bec-aws-bq/supabase_bq_transfer.log"
        logger.info(f"📝 Detailed Supabase transfer logs will be written to: {supabase_log_file}")
        
        try:
            # Execute Supabase to BigQuery transfer using Meltano
            logger.info("🚀 Starting Meltano supabase-to-bigquery pipeline...")
            logger.info(f"Working directory: ../bec-meltano")
            logger.info(f"Command: meltano run supabase-to-bigquery")
            
            supabase_result = subprocess.run([
                'bash', '-c', 
                'eval "$(conda shell.bash hook)" && conda activate bec && meltano run supabase-to-bigquery 2>&1 | tee ' + supabase_log_file
            ],
                capture_output=True,
                text=True,
                cwd="../bec-meltano"  # Use meltano directory
            )
            
            if supabase_result.returncode == 0:
                logger.info("✅ Supabase to BigQuery transfer completed successfully")
                logger.info("📋 Supabase transfer summary:")
                
                # Parse output to get table-specific information
                output_lines = supabase_result.stdout.split('\n')
                table_count = 0
                failed_tables = []
                successful_tables = []
                
                for line in output_lines:
                    if 'supabase_' in line and ('loaded' in line.lower() or 'inserted' in line.lower()):
                        table_count += 1
                        if 'error' in line.lower() or 'failed' in line.lower():
                            failed_tables.append(line.strip())
                        else:
                            successful_tables.append(line.strip())
                
                logger.info(f"   📊 Tables processed: {table_count}")
                logger.info(f"   ✅ Successful: {len(successful_tables)}")
                logger.info(f"   ❌ Failed: {len(failed_tables)}")
                
                if successful_tables:
                    logger.info("   📋 Successful table transfers:")
                    for table_info in successful_tables[:5]:  # Show first 5
                        logger.info(f"      ✓ {table_info}")
                    if len(successful_tables) > 5:
                        logger.info(f"      ... and {len(successful_tables) - 5} more")
                
                if failed_tables:
                    logger.warning("   ⚠️ Failed table transfers:")
                    for table_info in failed_tables:
                        logger.warning(f"      ❌ {table_info}")
                
                # Add Supabase tables to collections
                all_table_names.extend(supabase_tables)
                all_transfer_logs.append(f"SUPABASE: {len(successful_tables)} successful, {len(failed_tables)} failed")
                
                # Generate BigQuery table references for Supabase tables
                for table_name in supabase_tables:
                    bq_table_ref = f"olist_data_warehouse.supabase_{table_name}"
                    all_bq_tables.append(bq_table_ref)
                    
                logger.info(f"📁 Full transfer details saved to: {supabase_log_file}")
                    
            else:
                logger.error(f"❌ Supabase to BigQuery transfer failed with return code: {supabase_result.returncode}")
                logger.error("📋 Error details:")
                
                # Show error details
                error_lines = supabase_result.stderr.split('\n')
                for line in error_lines[-10:]:  # Show last 10 lines of error
                    if line.strip():
                        logger.error(f"   {line.strip()}")
                
                logger.error(f"📁 Check full error log at: {supabase_log_file}")
                all_transfer_logs.append(f"SUPABASE FAILED: {supabase_result.stderr[:200]}...")
                
        except Exception as e:
            logger.error(f"❌ Exception during Supabase transfer: {str(e)}")
            all_transfer_logs.append(f"SUPABASE ERROR: {str(e)}")
    else:
        logger.info("⚠️ No Supabase tables found to process")
        all_transfer_logs.append("SUPABASE: No tables found")
    
    # ===========================================
    # PHASE 2: Process RDS MySQL tables
    # ===========================================
    logger.info("🚀 PHASE 2: Processing RDS MySQL tables...")
    rds_tables = []
    
    try:
        # Discover tables directly from RDS
        logger.info("� Discovering tables from RDS...")
        import pymysql
        connection = pymysql.connect(
            host=config.rds_host,
            user=os.getenv('MYSQL_USERNAME'),
            password=os.getenv('MYSQL_PASSWORD'),
            database=os.getenv('MYSQL_DATABASE', 'bec_rds_db'),
            port=int(os.getenv('MYSQL_PORT', '3306'))
        )
        
        cursor = connection.cursor()
        cursor.execute("SHOW TABLES")
        tables = cursor.fetchall()
        rds_tables = [table[0] for table in tables if not table[0].startswith('rds_')]
        
        logger.info(f"📊 Discovered {len(rds_tables)} tables from RDS: {rds_tables}")
        
        cursor.close()
        connection.close()
        
    except Exception as rds_error:
        logger.warning(f"⚠️ Could not discover tables from RDS: {str(rds_error)}")
        
        # Fallback: try to get table names from S3 metadata
        if _1_s3_to_rds and isinstance(_1_s3_to_rds, list) and len(_1_s3_to_rds) > 0:
            logger.info("🔄 Using table names from S3 metadata as RDS fallback...")
            for s3_path in _1_s3_to_rds:
                filename = os.path.basename(s3_path).replace('.csv', '')
                table_name = filename.lower().replace('-', '_')
                rds_tables.append(table_name)
            logger.info(f"📊 Got {len(rds_tables)} table names from S3 metadata: {rds_tables}")
    
    # Process RDS tables if found
    if rds_tables:
        logger.info(f"🔄 Processing {len(rds_tables)} RDS tables for BigQuery transfer...")
        try:
            # Execute RDS to BigQuery transfer
            rds_result = subprocess.run([
                'bash', '-c', 
                'eval "$(conda shell.bash hook)" && conda activate bec && python main.py --stage rds-bq'
            ],
                capture_output=True,
                text=True,
                cwd=".."  # Run from parent directory
            )
            
            if rds_result.returncode == 0:
                logger.info("✅ RDS to BigQuery transfer completed successfully")
                logger.info(f"RDS transfer output: {rds_result.stdout}")
                
                # Add RDS tables to collections
                all_table_names.extend(rds_tables)
                all_transfer_logs.append(f"RDS: {rds_result.stdout}")
                
                # Generate BigQuery table references for RDS tables
                for table_name in rds_tables:
                    bq_table_ref = f"{config.bigquery_dataset}.rds_{table_name}"
                    all_bq_tables.append(bq_table_ref)
                    
            else:
                logger.warning(f"⚠️ RDS to BigQuery transfer failed: {rds_result.stderr}")
                all_transfer_logs.append(f"RDS FAILED: {rds_result.stderr}")
                
        except Exception as e:
            logger.warning(f"⚠️ Error during RDS transfer: {str(e)}")
            all_transfer_logs.append(f"RDS ERROR: {str(e)}")
    else:
        logger.info("⚠️ No RDS tables found to process")
        all_transfer_logs.append("RDS: No tables found")
    
    # ===========================================
    # FINAL: Combine results and return
    # ===========================================
    
    # Check if we have any tables processed
    if not all_table_names:
        logger.warning("⚠️ No tables found from either Supabase or RDS")
        return {
            "bq_tables": [],
            "dataset": config.bigquery_dataset,
            "s3_paths": s3_paths_for_metadata,
            "table_names": [],
            "supabase_tables": supabase_tables,
            "rds_tables": rds_tables,
            "transfer_log": "; ".join(all_transfer_logs),
            "status": "warning"
        }
    
    # Create comprehensive result
    transfer_result = {
        "bq_tables": all_bq_tables,
        "dataset": config.bigquery_dataset,
        "s3_paths": s3_paths_for_metadata,
        "table_names": all_table_names,
        "supabase_tables": supabase_tables,
        "rds_tables": rds_tables,
        "transfer_log": "; ".join(all_transfer_logs),
        "status": "success"
    }
    
    # Log final metadata for tracking
    logger.info("🎉 Dual database transfer completed!")
    logger.info(f"📊 Total tables processed: {len(all_table_names)}")
    logger.info(f"📊 Supabase tables: {len(supabase_tables)}")
    logger.info(f"📊 RDS tables: {len(rds_tables)}")
    logger.info(f"📊 BigQuery tables created: {len(all_bq_tables)}")
    logger.info(f"📊 BigQuery dataset: {config.bigquery_dataset}")
    
    return transfer_result


@asset(group_name="Transformation", deps=[_2_staging_to_bigquery])
def _3_process_datawarehouse(config: PipelineConfig, _1_s3_to_rds, _2_staging_to_bigquery: Dict[str, Any]) -> Dict[str, Any]:
    """
   Transforming data warehouse for Complete dashboard visualization
    
    Args:
        _1_s3_to_rds: Result from S3 to RDS stage (may be None if failed)
        _2_staging_to_bigquery: Result from RDS to BigQuery stage

    Returns:
        Transforming data warehouse for Comlete dashboard visualization
    """

    logger = get_dagster_logger()

    logger.info("🔄 Transformation - Processing BigQuery using DBT...")
    
    # Get information from _2_staging_to_bigquery result (primary source)
    bq_tables = _2_staging_to_bigquery.get("bq_tables", [])
    table_names = _2_staging_to_bigquery.get("table_names", [])
    s3_paths = _2_staging_to_bigquery.get("s3_paths", [])
    
    # Try to get additional info from _1_s3_to_rds if available
    source_files_count = len(s3_paths)
    csv_files = [os.path.basename(path) for path in s3_paths]
    
    if _1_s3_to_rds and isinstance(_1_s3_to_rds, list):
        logger.info(f"✅ Additional data available from _1_s3_to_rds stage: {len(_1_s3_to_rds)} files")
        # Use _1_s3_to_rds data if available and more complete
        if len(_1_s3_to_rds) > len(s3_paths):
            s3_paths = _1_s3_to_rds
            csv_files = [os.path.basename(path) for path in _1_s3_to_rds]
            source_files_count = len(_1_s3_to_rds)
    else:
        logger.info("⚠️ _1_s3_to_rds stage failed or returned no data - using data from _2_staging_to_bigquery")

    # Create summary based on available information
    summary = {
        "pipeline_status": "completed",
        "source_files": source_files_count,
        "s3_files_uploaded": len(s3_paths),
        "rds_tables_created": len(table_names),
        "bq_tables_transferred": len(bq_tables),
        "csv_files": csv_files,
        "s3_paths": s3_paths,
        "rds_tables": table_names,
        "bq_tables": bq_tables
    }
    
    logger.info("🎉 Visualization completed successfully!")
    logger.info(f"Processed {source_files_count} files through the pipeline")
    logger.info(f"BigQuery tables available: {len(bq_tables)}")
    logger.info(f"RDS tables: {len(table_names)}")
    
    # Log summary metadata
    logger.info(f"Visualization status: success")
    logger.info(f"Stages completed: 3")
    logger.info(f"Final destination: Reports")
    
    return summary

@asset(group_name="Visualization", deps=[_3_process_datawarehouse])
def _4_bigquery_to_visualization(config: PipelineConfig, _1_s3_to_rds: Dict[str, Any], _2_staging_to_bigquery: Dict[str, Any], _3_process_datawarehouse: Dict[str, Any]) -> Dict[str, Any]:
    """
    Generate a comprehensive summary of the entire pipeline execution
    
    Returns:
        Complete pipeline execution summary with all metrics
    """
    logger = get_dagster_logger()

    logger.info("🔄 Visualization - Processing Data for Visualization...")

    summary = {
        "pipeline_status": "completed",
        "source_files": len(_1_s3_to_rds["csv_files"]),
        "s3_files_uploaded": len(_1_s3_to_rds["s3_paths"]),
        "rds_tables_created": len(_1_s3_to_rds["table_names"]),
        "bq_tables_transferred": len(_2_staging_to_bigquery["bq_tables"]),        
        "csv_files": _1_s3_to_rds["csv_files"],
        "s3_paths": _1_s3_to_rds["s3_paths"],
        "rds_tables": _1_s3_to_rds["table_names"],
        "bq_tables": _2_staging_to_bigquery["bq_tables"]
    }
    
    logger.info("🎉 Pipeline completed successfully!")
    logger.info(f"Processed {len(_1_s3_to_rds['csv_files'])} CSV files through the entire pipeline")

    # Log summary metadata
    logger.info(f"Pipeline status: success")
    logger.info(f"Total files processed: {len(_1_s3_to_rds['csv_files'])}")
    logger.info(f"Stages completed: 3")
    logger.info(f"Final destination: BigQuery")
    
    return summary


@job(name="s3_rds_bigquery_pipeline")
def s3_rds_bq_pipeline():
    """
    Complete ETL pipeline: CSV → S3 → RDS → BigQuery
    
    This job orchestrates the entire data pipeline with proper dependencies
    and comprehensive monitoring of each stage.
    """
    # The asset dependencies are automatically handled by Dagster
    _4_bigquery_to_visualization()


# Define the Dagster definitions
defs = Definitions(
    assets=[
        _1_s3_to_rds,  # Merged asset combining S3 upload and RDS import
        _2_staging_to_bigquery,
        _3_process_datawarehouse,
        _4_bigquery_to_visualization
    ],
    jobs=[s3_rds_bq_pipeline]
)


if __name__ == "__main__":
    # For testing - you can run individual assets or the full pipeline
    from dagster import materialize
    
    print("🚀 Running S3-RDS-BigQuery Pipeline with Dagster")
    print("=" * 60)
    
    # Test Supabase connection and table discovery first
    print("🔄 Testing Supabase PostgreSQL connection and table discovery...")
    
    # Use the same connection parameters as in the asset function
    connection_params = {
        'host': "aws-1-ap-southeast-1.pooler.supabase.com",
        'port': 5432, 
        'database': "postgres",
        'user': "postgres.royhmnxmsfichopabwsi",
        'password': os.getenv("TAP_POSTGRES_PASSWORD")
    }
    
    try:
        # Test connection and get tables
        conn = psycopg2.connect(**connection_params)
        cursor = conn.cursor()
        
        # Query for tables
        cursor.execute("""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'public' 
            AND table_type = 'BASE TABLE'
            ORDER BY table_name;
        """)
        
        supabase_tables = [row[0] for row in cursor.fetchall()]
        
        cursor.close()
        conn.close()
        
        print(f"✅ Found {len(supabase_tables)} Supabase tables: {supabase_tables}")
        
        if supabase_tables:
            print("🔄 Now testing Meltano supabase-bq pipeline...")
            try:
                # Test meltano supabase-bq pipeline
                result = subprocess.run(
                    ['meltano', 'run', 'supabase-bq'],
                    capture_output=True,
                    text=True,
                    cwd="../RDS-BQ",  # Path to meltano directory
                    timeout=300
                )
                
                if result.returncode == 0:
                    print("✅ Meltano supabase-bq pipeline completed successfully!")
                    print(f"Output: {result.stdout}")
                else:
                    print(f"❌ Meltano supabase-bq pipeline failed: {result.stderr}")
                    
            except Exception as e:
                print(f"⚠️ Error running Meltano pipeline: {str(e)}")
        else:
            print("❌ No Supabase tables found - cannot test pipeline")
            
    except Exception as e:
        print(f"❌ Supabase connection failed: {str(e)}")
    
    print("\n" + "=" * 60)
    
    # Run both assets since staging depends on S3 to RDS
    print("🔄 Running S3 to RDS and then Staging to BigQuery transfer...")
    result = materialize([_1_s3_to_rds, _2_staging_to_bigquery])
    
    if result.success:
        print("✅ Dagster pipeline completed successfully!")
    else:
        print("❌ Dagster pipeline failed!")
    
    if result.success:
        print("✅ Pipeline completed successfully!")
    else:
        print("❌ Pipeline failed!")
        for event in result.events_for_node:
            if event.event_type_value == "STEP_FAILURE":
                print(f"Error: {event}")
