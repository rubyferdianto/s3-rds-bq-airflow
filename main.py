#!/usr/bin/env python3
"""
Main orchestrator for s3-rds-bq-airflow data pipeline
Complete end-to-end data pipeline:
1. Database setup and configuration
2. CSV to RDS MySQL import (local and S3)
3. RDS MySQL to BigQuery transfer
"""

import os
import sys
import subprocess
import logging
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def run_script(script_path, description, cwd=None):
    """Run a Python script and return success status"""
    logger.info(f"🚀 Starting: {description}")
    logger.info(f"📜 Executing: {script_path}")
    
    try:
        # Determine the working directory and script path
        if cwd:
            # If cwd is specified, the script path should be relative to cwd
            full_script_path = os.path.join(cwd, os.path.basename(script_path))
            working_dir = cwd
        else:
            # Use the script path as provided
            full_script_path = script_path
            working_dir = os.getcwd()
        
        # Run the script using the same Python interpreter
        result = subprocess.run([sys.executable, full_script_path], 
                              capture_output=True, 
                              text=True, 
                              cwd=working_dir)
        
        if result.returncode == 0:
            logger.info(f"✅ {description} completed successfully")
            if result.stdout:
                # Log only the summary lines to avoid too much output
                lines = result.stdout.strip().split('\n')
                summary_lines = [line for line in lines if any(marker in line for marker in ['✅', '❌', '🎉', '📊', '💡'])]
                if summary_lines:
                    logger.info("📋 Key results:")
                    for line in summary_lines[-10:]:  # Show last 10 important lines
                        logger.info(f"   {line}")
            return True
        else:
            logger.error(f"❌ {description} failed with return code {result.returncode}")
            if result.stderr:
                logger.error(f"Error output:\n{result.stderr}")
            if result.stdout:
                logger.info(f"Standard output:\n{result.stdout}")
            return False
            
    except Exception as e:
        logger.error(f"❌ Failed to execute {script_path}: {str(e)}")
        return False

def check_environment_variables():
    """Check if required environment variables are set"""
    required_vars = [
        'MYSQL_HOST', 'MYSQL_USERNAME', 'MYSQL_PASSWORD', 'MYSQL_DATABASE',
        'GCP_PROJECT', 'BQ_DATASET', 'GOOGLE_APPLICATION_CREDENTIALS_JSON'
    ]
    missing_vars = [var for var in required_vars if not os.getenv(var)]
    
    if missing_vars:
        logger.error("❌ Missing required environment variables!")
        logger.error(f"💡 Please set: {', '.join(missing_vars)}")
        logger.error("   Example in .env file:")
        logger.error("   # MySQL Configuration")
        logger.error("   MYSQL_HOST=your-rds-endpoint.region.rds.amazonaws.com")
        logger.error("   MYSQL_USERNAME=your_username")
        logger.error("   MYSQL_PASSWORD=your_password")
        logger.error("   MYSQL_DATABASE=your_database_name")
        logger.error("   # BigQuery Configuration")
        logger.error("   GCP_PROJECT=your-project-id")
        logger.error("   BQ_DATASET=your_dataset_name")
        logger.error("   GOOGLE_APPLICATION_CREDENTIALS_JSON='{...}'")
        return False
    
    logger.info("✅ Environment variables check passed")
    return True

def main():
    """Main orchestrator function"""
    logger.info("=" * 80)
    logger.info("🎯 STARTING COMPLETE S3-RDS-BIGQUERY DATA PIPELINE")
    logger.info("=" * 80)
    
    # Step 0: Check environment variables
    logger.info("=" * 80)
    logger.info("STEP 0: ENVIRONMENT VALIDATION")
    logger.info("=" * 80)
    
    if not check_environment_variables():
        logger.error("❌ Environment validation failed. Please fix and try again.")
        sys.exit(1)
    
    # Check if all required scripts exist
    required_scripts = [
        ("CSV-RDS/setup-database.py", "Database Setup Script"),
        ("CSV-RDS/csv-to-rds-via-s3.py", "Local CSV to RDS Import Script"),
        ("CSV-RDS/s3-to-rds.py", "S3 to RDS Import Script"),
        ("RDS-BQ/run-pipeline.py", "RDS to BigQuery Transfer Script")
    ]
    
    missing_scripts = []
    for script_path, description in required_scripts:
        if not os.path.exists(script_path):
            missing_scripts.append(f"{script_path} ({description})")
    
    if missing_scripts:
        logger.error(f"❌ Missing required scripts:")
        for missing in missing_scripts:
            logger.error(f"   - {missing}")
        sys.exit(1)
    
    logger.info("✅ All required scripts found")
    
    # Step 1: Database setup and configuration
    logger.info("=" * 80)
    logger.info("STEP 1: DATABASE SETUP AND CONFIGURATION")
    logger.info("=" * 80)
    
    setup_success = run_script('CSV-RDS/setup-database.py', 'RDS MySQL database setup and configuration')
    if not setup_success:
        logger.error("❌ Database setup failed. Stopping pipeline.")
        logger.error("💡 Please check your database credentials and connectivity.")
        sys.exit(1)
    
    logger.info("✅ Database is ready for data import!")
    
    # Step 2: Local CSV to RDS import
    logger.info("=" * 80)
    logger.info("STEP 2: LOCAL CSV TO RDS IMPORT")
    logger.info("=" * 80)

    csv_success = run_script('CSV-RDS/csv-to-rds-via-s3.py', 'Local CSV to RDS import workflow')
    if not csv_success:
        logger.warning("⚠️ Local CSV to RDS import had issues, but continuing with S3 import")

    # Step 3: S3 to RDS import
    logger.info("=" * 80)
    logger.info("STEP 3: S3 TO RDS IMPORT (DIRECT READING)")
    logger.info("=" * 80)

    s3_success = run_script('CSV-RDS/s3-to-rds.py', 'S3 to RDS import using direct pandas reading')
    if not s3_success:
        logger.warning("⚠️ S3 to RDS import failed, but continuing to BigQuery step")

    # Step 4: RDS to BigQuery transfer
    logger.info("=" * 80)
    logger.info("STEP 4: RDS TO BIGQUERY TRANSFER")
    logger.info("=" * 80)

    bq_success = run_script('RDS-BQ/run-pipeline.py', 'RDS MySQL to BigQuery transfer')
    if not bq_success:
        logger.error("⚠️ RDS to BigQuery transfer failed or no data found")

    # Final summary
    logger.info("=" * 80)
    logger.info("📊 COMPLETE PIPELINE EXECUTION SUMMARY")
    logger.info("=" * 80)
    
    logger.info("✅ Environment validation: PASSED")
    logger.info("✅ Database setup: PASSED")
    logger.info(f"{'✅' if csv_success else '❌'} Local CSV to RDS import: {'PASSED' if csv_success else 'FAILED'}")
    logger.info(f"{'✅' if s3_success else '❌'} S3 to RDS import: {'PASSED' if s3_success else 'FAILED'}")
    logger.info(f"{'✅' if bq_success else '❌'} RDS to BigQuery transfer: {'PASSED' if bq_success else 'FAILED'}")
    
    # Calculate overall status
    data_imported = csv_success or s3_success
    
    if data_imported and bq_success:
        logger.info("=" * 80)
        logger.info("🎉 COMPLETE PIPELINE SUCCESSFULLY EXECUTED!")
        logger.info("=" * 80)
        logger.info("💡 Your data journey: CSV → RDS MySQL → BigQuery")
        logger.info(f"� Check your BigQuery dataset: {os.getenv('GCP_PROJECT')}.{os.getenv('BQ_DATASET')}")
        logger.info("✨ Data is now ready for analytics in BigQuery!")
        return 0
    elif data_imported:
        logger.info("=" * 80)
        logger.info("🎯 PARTIAL SUCCESS: Data imported to RDS")
        logger.info("=" * 80)
        logger.info("💡 CSV data successfully imported to RDS MySQL")
        logger.info("❌ BigQuery transfer failed - please check BigQuery configuration")
        return 1
    elif bq_success:
        logger.info("=" * 80)
        logger.info("🎯 PARTIAL SUCCESS: BigQuery transfer completed")
        logger.info("=" * 80)
        logger.info("💡 Existing RDS data transferred to BigQuery")
        logger.info("❌ CSV import failed - please check CSV/S3 configuration")
        return 0
    else:
        logger.error("=" * 80)
        logger.error("❌ PIPELINE FAILED: No successful data transfers")
        logger.error("=" * 80)
        logger.error("💡 Please check the error messages above and fix issues")
        return 1

if __name__ == "__main__":
    try:
        exit_code = main()
        sys.exit(exit_code)
    except KeyboardInterrupt:
        logger.info("\n🛑 Pipeline interrupted by user")
        sys.exit(130)
    except Exception as e:
        logger.error(f"❌ Unexpected error in pipeline: {str(e)}")
        sys.exit(1)
