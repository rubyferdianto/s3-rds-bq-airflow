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


@asset(group_name="Ingestion")
def _1_local_csv_to_s3(config: PipelineConfig) -> List[str]:
    """
    Extract CSV files from local directory and upload them to S3
    
    Returns:
        List of local CSV file paths that were uploaded to S3
    """
    logger = get_dagster_logger()
    
    logger.info("🔄 Ingestion - Uploading CSV files to S3...")

    csv_dir = Path(config.csv_directory)
    logger.info(f"Local CSV directory: {csv_dir}")

    if not csv_dir.exists():
        raise FileNotFoundError(f"CSV local directory not found: {csv_dir}")
    
    # Find all CSV files
    csv_files = list(csv_dir.glob("*.csv"))
    
    if not csv_files:
        raise ValueError(f"No CSV files found in local directory")
    
    file_paths = [str(f) for f in csv_files]
    logger.info(f"Found {len(file_paths)} CSV files: {file_paths}")
    
    # Execute CSV to S3 upload
    try:
        s3_upload_result = subprocess.run(
            ["python", "main.py", "--stage", "csv-s3"],
            capture_output=True,
            text=True,
            cwd=".."  # Run from parent directory
        )
        
        if s3_upload_result.returncode != 0:
            logger.error(f"CSV to S3 upload failed: {s3_upload_result.stderr}")
            raise Exception(f"S3 upload failed with exit code {s3_upload_result.returncode}")
        
        logger.info("✅ CSV to S3 upload completed successfully")
        logger.info(f"S3 upload output: {s3_upload_result.stdout}")
        
    except Exception as e:
        logger.error(f"Error during S3 upload: {str(e)}")
        raise       

    # Create metadata about the files (using absolute paths)
    file_metadata = {}
    total_size = 0
    for file_path in file_paths:
        try:
            # Convert to absolute path to ensure we can access the file
            abs_file_path = os.path.abspath(file_path)
            if os.path.exists(abs_file_path):
                file_size = os.path.getsize(abs_file_path)
                file_metadata[file_path] = {"size_bytes": file_size}
                total_size += file_size
                logger.info(f"File: {file_path}, Size: {file_size} bytes")
            else:
                logger.warning(f"File not found for metadata: {abs_file_path}")
        except Exception as e:
            logger.warning(f"Could not get size for {file_path}: {str(e)}")

    # Log metadata for tracking
    logger.info(f"Total files: {len(file_paths)}")
    logger.info(f"Total size: {round(total_size / 1024 / 1024, 2)} MB")
    
    return file_paths


@asset(group_name="Extraction")
def _2_s3_to_rds(config: PipelineConfig) -> List[str]:
    """
    Move existing CSV files from S3 and into RDS tables
    This function directly lists files from S3 bucket without depending on local CSV listing

    Returns:
        List of S3 paths for CSV files that were processed
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
        rds_result = subprocess.run(
            ["python", "main.py", "--stage", "s3-rds"],
            capture_output=True,
            text=True,
            cwd=".."  # Run from parent directory
        )
        
        if rds_result.returncode != 0:
            logger.error(f"S3 to RDS import failed: {rds_result.stderr}")
            raise Exception(f"RDS import failed with exit code {rds_result.returncode}")
        
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


@asset(group_name="Loading", deps=[_2_s3_to_rds])
def _3_rds_to_bigquery(config: PipelineConfig, _2_s3_to_rds) -> Dict[str, Any]:
    """
    Loading AWS RDS MySQL into Google Cloud BigQuery
    
    This function depends on _2_s3_to_rds but can handle failures gracefully.
    If _2_s3_to_rds fails, it will still attempt to get data from RDS.

    Args:
        _2_s3_to_rds: Result from previous stage (may be None if failed)

    Returns:
        Loading AWS RDS MySQL into Google Cloud BigQuery
    """
    logger = get_dagster_logger()

    logger.info("🔄 Loading - RDS to BigQuery...")
    logger.info("💡 This function can handle previous stage failures")
    
    # Try to get table names from previous stage, but handle failure gracefully
    table_names = []
    s3_paths_for_metadata = []
    
    try:
        # Check if we have valid data from _2_s3_to_rds
        if _2_s3_to_rds and isinstance(_2_s3_to_rds, list) and len(_2_s3_to_rds) > 0:
            logger.info("✅ Using table names from successful _2_s3_to_rds stage")
            for s3_path in _2_s3_to_rds:
                filename = os.path.basename(s3_path).replace('.csv', '')
                table_name = filename.lower().replace('-', '_')
                table_names.append(table_name)
                s3_paths_for_metadata.append(s3_path)
            logger.info(f"📊 Got {len(table_names)} tables from previous stage: {table_names}")
        else:
            logger.warning("⚠️ Previous stage _2_s3_to_rds failed or returned empty result")
            logger.info("🔄 Attempting alternative table discovery methods...")
            
            # Fallback method 1: Check S3 imported folder for processed files
            try:
                s3_client = boto3.client('s3')
                logger.info(f"📁 Checking S3 imported folder: {config.s3_imported_prefix}")
                response = s3_client.list_objects_v2(
                    Bucket=config.s3_bucket,
                    Prefix=f"{config.s3_imported_prefix}"
                )
                
                if 'Contents' in response:
                    logger.info("📁 Found processed files in S3 imported folder")
                    for obj in response['Contents']:
                        key = obj['Key']
                        if key.lower().endswith('.csv') and key != f"{config.s3_imported_prefix}":
                            filename = os.path.basename(key).replace('.csv', '')
                            table_name = filename.lower().replace('-', '_')
                            table_names.append(table_name)
                            s3_paths_for_metadata.append(key)
                
                # Fallback method 2: Check S3 source folder
                if not table_names:
                    logger.info("📁 No files in imported folder, checking source folder...")
                    response = s3_client.list_objects_v2(
                        Bucket=config.s3_bucket,
                        Prefix=f"{config.s3_source_prefix}"
                    )
                    
                    if 'Contents' in response:
                        for obj in response['Contents']:
                            key = obj['Key']
                            if key.lower().endswith('.csv') and key != f"{config.s3_source_prefix}":
                                filename = os.path.basename(key).replace('.csv', '')
                                table_name = filename.lower().replace('-', '_')
                                table_names.append(table_name)
                                s3_paths_for_metadata.append(key)
                
                logger.info(f"📊 Found {len(table_names)} tables from S3: {table_names}")
                
            except Exception as s3_error:
                logger.warning(f"⚠️ S3 fallback failed: {str(s3_error)}")
            
            # Fallback method 3: Discover tables directly from RDS
            if not table_names:
                logger.info("🔄 No S3 files found, attempting to discover tables from RDS...")
                try:
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
                    table_names = [table[0] for table in tables if not table[0].startswith('sys_')]
                    
                    logger.info(f"📊 Discovered {len(table_names)} tables from RDS: {table_names}")
                    
                    cursor.close()
                    connection.close()
                    
                except Exception as rds_error:
                    logger.error(f"❌ Could not discover tables from RDS: {str(rds_error)}")
                    # Don't raise here - let the function continue with empty tables
                    logger.warning("⚠️ Continuing with empty table list")
        
    except Exception as e:
        logger.warning(f"⚠️ Error during table discovery: {str(e)}")
        logger.info("🔄 Continuing with empty table list")
        table_names = []
    
    if not table_names:
        logger.warning("⚠️ No tables found for BigQuery transfer")
        return {
            "bq_tables": [],
            "dataset": config.bigquery_dataset,
            "s3_paths": s3_paths_for_metadata,
            "table_names": [],
            "transfer_log": "No tables found for transfer",
            "status": "warning"
        }
    
    logger.info(f"Starting transfer of {len(table_names)} tables from RDS to BigQuery")
    
    # Execute the RDS to BigQuery stage using existing main.py
    try:
        result = subprocess.run(
            ["python", "main.py", "--stage", "rds-bq"],
            capture_output=True,
            text=True,
            cwd=".."  # Run from parent directory
        )
        
        if result.returncode != 0:
            logger.error(f"RDS to BigQuery transfer failed: {result.stderr}")
            raise Exception(f"Transfer failed with exit code {result.returncode}")
        
        logger.info("RDS to BigQuery transfer completed successfully")
        logger.info(f"Transfer output: {result.stdout}")
        
        # Generate BigQuery table references
        bq_tables = []
        for table_name in table_names:
            bq_table_ref = f"{config.bigquery_dataset}.{table_name}"
            bq_tables.append(bq_table_ref)
        
        transfer_result = {
            "bq_tables": bq_tables,
            "dataset": config.bigquery_dataset,
            "s3_paths": s3_paths_for_metadata,
            "table_names": table_names,
            "transfer_log": result.stdout,
            "status": "success"
        }
        
        # Log metadata for tracking
        logger.info(f"Tables transferred: {len(bq_tables)}")
        logger.info(f"BigQuery dataset: {config.bigquery_dataset}")
        
        return transfer_result
        
    except Exception as e:
        logger.error(f"Error during BigQuery transfer: {str(e)}")
        raise


@asset(group_name="Transformation", deps=[_3_rds_to_bigquery])
def _4_process_datawarehouse(config: PipelineConfig, _2_s3_to_rds, _3_rds_to_bigquery: Dict[str, Any]) -> Dict[str, Any]:
    """
    Transforming data warehouse for Complete dashboard visualization
    
    Args:
        _2_s3_to_rds: Result from S3 to RDS stage (may be None if failed)
        _3_rds_to_bigquery: Result from RDS to BigQuery stage
    
    Returns:
        Transforming data warehouse for Complete dashboard visualization
    """
    logger = get_dagster_logger()

    logger.info("🔄 Transformation - Processing BigQuery using DBT...")
    
    # Get information from _3_rds_to_bigquery result (primary source)
    bq_tables = _3_rds_to_bigquery.get("bq_tables", [])
    table_names = _3_rds_to_bigquery.get("table_names", [])
    s3_paths = _3_rds_to_bigquery.get("s3_paths", [])
    
    # Try to get additional info from _2_s3_to_rds if available
    source_files_count = len(s3_paths)
    csv_files = [os.path.basename(path) for path in s3_paths]
    
    if _2_s3_to_rds and isinstance(_2_s3_to_rds, list):
        logger.info(f"✅ Additional data available from _2_s3_to_rds stage: {len(_2_s3_to_rds)} files")
        # Use _2_s3_to_rds data if available and more complete
        if len(_2_s3_to_rds) > len(s3_paths):
            s3_paths = _2_s3_to_rds
            csv_files = [os.path.basename(path) for path in _2_s3_to_rds]
            source_files_count = len(_2_s3_to_rds)
    else:
        logger.info("⚠️ _2_s3_to_rds stage failed or returned no data - using data from _3_rds_to_bigquery")
    
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

@asset(group_name="Visualization", deps=[_4_process_datawarehouse])
def _5_bigquery_to_visualization(config: PipelineConfig, _2_s3_to_rds: Dict[str, Any], _3_rds_to_bigquery: Dict[str, Any], _4_process_datawarehouse: Dict[str, Any]) -> Dict[str, Any]:
    """
    Generate a comprehensive summary of the entire pipeline execution
    
    Returns:
        Complete pipeline execution summary with all metrics
    """
    logger = get_dagster_logger()

    logger.info("🔄 Visualization - Processing Data for Visualization...")

    summary = {
        "pipeline_status": "completed",
        "source_files": len(_2_s3_to_rds["csv_files"]),
        "s3_files_uploaded": len(_2_s3_to_rds["s3_paths"]),
        "rds_tables_created": len(_2_s3_to_rds["table_names"]),
        "bq_tables_transferred": len(_3_rds_to_bigquery["bq_tables"]),        
        "csv_files": _2_s3_to_rds["csv_files"],
        "s3_paths": _2_s3_to_rds["s3_paths"],
        "rds_tables": _2_s3_to_rds["table_names"],
        "bq_tables": _3_rds_to_bigquery["bq_tables"]
    }
    
    logger.info("🎉 Pipeline completed successfully!")
    logger.info(f"Processed {len(_2_s3_to_rds['csv_files'])} CSV files through the entire pipeline")
    
    # Log summary metadata
    logger.info(f"Pipeline status: success")
    logger.info(f"Total files processed: {len(_2_s3_to_rds['csv_files'])}")
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
    _5_bigquery_to_visualization()


# Define the Dagster definitions
defs = Definitions(
    assets=[
        _1_local_csv_to_s3,
        _2_s3_to_rds,  # Merged asset combining S3 upload and RDS import
        _3_rds_to_bigquery,
        _4_process_datawarehouse,
        _5_bigquery_to_visualization
    ],
    jobs=[s3_rds_bq_pipeline]
)


if __name__ == "__main__":
    # For testing - you can run individual assets or the full pipeline
    from dagster import materialize
    
    print("🚀 Running S3-RDS-BigQuery Pipeline with Dagster")
    print("=" * 60)
    
    # Run the complete pipeline
    result = materialize(
        [_1_local_csv_to_s3,
         _2_s3_to_rds, 
         _3_rds_to_bigquery, 
         _4_process_datawarehouse, 
         _5_bigquery_to_visualization],
        run_config={
            "resources": {
                "config": {
                    "csv_directory": "../bec-aws-bq/csv-source-file",  # Updated path from DAGSTER folder
                    "s3_bucket": os.getenv("S3_BUCKET", "bec-bucket-aws"),
                    "s3_source_prefix": os.getenv("S3_SOURCE_PREFIX", "s3-to-rds"),
                    "s3_imported_prefix": os.getenv("S3_IMPORTED_PREFIX", "s3-imported-to-rds"),
                    "rds_host": os.getenv("MYSQL_HOST", "bec-database.c123456789.us-east-1.rds.amazonaws.com"),
                    "bigquery_dataset": os.getenv("BQ_DATASET", "bec_dataset")
                }
            }
        }
    )
    
    if result.success:
        print("✅ Pipeline completed successfully!")
    else:
        print("❌ Pipeline failed!")
        for event in result.events_for_node:
            if event.event_type_value == "STEP_FAILURE":
                print(f"Error: {event}")
