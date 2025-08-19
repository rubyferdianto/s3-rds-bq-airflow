#!/usr/bin/env python3
"""
Main orchestrator for s3-aurora-bq-airflow data pipeline
1. Executes setup-database.py to create and configure RDS MySQL database
2. Executes CSV-AURORA scripts for data import workflows
3. Provides comprehensive pipeline execution with proper error handling
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
        # Run the script using the same Python interpreter
        result = subprocess.run([sys.executable, script_path], 
                              capture_output=True, 
                              text=True, 
                              cwd=cwd or os.getcwd())
        
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
    required_vars = ['MYSQL_HOST', 'MYSQL_USERNAME', 'MYSQL_PASSWORD']
    missing_vars = [var for var in required_vars if not os.getenv(var)]
    
    if missing_vars:
        logger.error("❌ Missing required environment variables!")
        logger.error(f"💡 Please set: {', '.join(missing_vars)}")
        logger.error("   Example in .env file:")
        logger.error("   MYSQL_HOST=your-rds-endpoint.region.rds.amazonaws.com")
        logger.error("   MYSQL_USERNAME=your_username")
        logger.error("   MYSQL_PASSWORD=your_password")
        logger.error("   MYSQL_DATABASE=your_database_name")
        return False
    
    logger.info("✅ Environment variables check passed")
    return True

def main():
    """Main orchestrator function"""
    logger.info("=" * 70)
    logger.info("🎯 STARTING S3-AURORA-BQ-AIRFLOW DATA PIPELINE")
    logger.info("=" * 70)
    
    # Step 0: Check environment variables
    logger.info("=" * 70)
    logger.info("STEP 0: ENVIRONMENT VALIDATION")
    logger.info("=" * 70)
    
    if not check_environment_variables():
        logger.error("❌ Environment validation failed. Please fix and try again.")
        sys.exit(1)
    
    # Check if all required scripts exist
    required_scripts = [
        ("CSV-RDS/setup-database.py", "Database Setup Script"),
        ("CSV-RDS/csv-to-rds-via-s3.py", "Local CSV to RDS Import Script"),
        ("CSV-RDS/s3-to-rds.py", "S3 to RDS Import Script"),
        ("RDS-BQ/run-pipeline.py", "RDS to BigQuery Import Script")
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
    logger.info("=" * 70)
    logger.info("STEP 1: DATABASE SETUP AND CONFIGURATION")
    logger.info("=" * 70)
    
    setup_success = run_script('CSV-RDS/setup-database.py', 'RDS MySQL database setup and configuration')
    if not setup_success:
        logger.error("❌ Database setup failed. Stopping pipeline.")
        logger.error("💡 Please check your database credentials and connectivity.")
        sys.exit(1)
    
    logger.info("✅ Database is ready for data import!")
    
    # Step 1: Local CSV to RDS import
    logger.info("=" * 70)
    logger.info("STEP 1: LOCAL CSV TO RDS IMPORT")
    logger.info("=" * 70)

    csv_success = run_script('CSV-RDS/csv-to-rds-via-s3.py', 'Local CSV to RDS import workflow')
    if not csv_success:
        logger.warning("⚠️ Local CSV to RDS import had issues, but continuing with S3 import")

    # Step 2: S3 to RDS import
    logger.info("=" * 70)
    logger.info("STEP 2: S3 TO RDS IMPORT (DIRECT PANDAS READING)")
    logger.info("=" * 70)

    s3_success = run_script('CSV-RDS/s3-to-rds.py', 'S3 to RDS import using direct pandas reading')
    if not s3_success:
        logger.warning("⚠️ S3 to RDS import failed")

    # Step 3: RDS to BigQuery import
    logger.info("=" * 70)
    logger.info("STEP 3: RDS TO BIGQUERY IMPORT (MELTANO)")
    logger.info("=" * 70)

    bq_success = run_script('RDS-BQ/run-pipeline.py', 'RDS MySQL to BigQuery import using Meltano', cwd='RDS-BQ')
    if not bq_success:
        logger.warning("⚠️ RDS to BigQuery import failed")

    # Final summary
    logger.info("=" * 70)
    logger.info("📊 PIPELINE EXECUTION SUMMARY")
    logger.info("=" * 70)
    
    logger.info("✅ Environment validation: PASSED")
    logger.info("✅ Database setup: PASSED")
    logger.info(f"{'✅' if csv_success else '❌'} Local CSV to RDS import: {'PASSED' if csv_success else 'FAILED'}")
    logger.info(f"{'✅' if s3_success else '❌'} S3 to RDS import: {'PASSED' if s3_success else 'FAILED'}")
    
    # Calculate overall status
    if s3_success and csv_success:
        logger.info("🎉 All pipeline steps completed successfully!")
        logger.info("💡 Your data has been imported to RDS MySQL!")
        return 0
    elif csv_success:
        logger.info("🎯 Pipeline completed with S3 import issues (local CSV import successful)")
        logger.info("💡 Local CSV data has been imported to RDS MySQL")
        return 0
    elif s3_success:
        logger.info("🎯 Pipeline completed with local CSV import issues (S3 import successful)")
        logger.info("💡 S3 data has been imported to RDS MySQL")
        return 0
    else:
        logger.error("❌ Pipeline completed with errors in both import methods")
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
