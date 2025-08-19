#!/usr/bin/env python3
"""
Main orchestrator for data pipeline
1. Executes setup-database.py to create and configure database
2. Executes s3_to_rds to load files from S3 to RDS MySQL
3. Executes csv_to_rds to load local CSV files to RDS MySQL
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

def run_script(script_name, description):
    """Run a Python script and return success status"""
    logger.info(f"🚀 Starting: {description}")
    logger.info(f"📜 Executing: {script_name}")
    
    try:
        # Run the script using the same Python interpreter
        result = subprocess.run([sys.executable, script_name], 
                              capture_output=True, 
                              text=True, 
                              cwd=os.getcwd())
        
        if result.returncode == 0:
            logger.info(f"✅ {description} completed successfully")
            if result.stdout:
                logger.info(f"Output:\n{result.stdout}")
            return True
        else:
            logger.error(f"❌ {description} failed with return code {result.returncode}")
            if result.stderr:
                logger.error(f"Error output:\n{result.stderr}")
            if result.stdout:
                logger.info(f"Standard output:\n{result.stdout}")
            return False
            
    except Exception as e:
        logger.error(f"❌ Failed to execute {script_name}: {str(e)}")
        return False

def main():
    """Main orchestrator function"""
    logger.info("=" * 60)
    logger.info("🎯 STARTING DATA PIPELINE ORCHESTRATOR")
    logger.info("=" * 60)
    
    # Check if all required scripts exist
    required_scripts = [
        ("setup-database.py", "STEP 0: Database Setup"),
        ("s3-to-rds.py", "STEP 1: S3 to RDS Import"),
        ("csv-to-rds-via-s3.py", "STEP 2: Local CSV to RDS Import (via S3)")
    ]
    
    missing_scripts = []
    for script_name, _ in required_scripts:
        if not os.path.exists(script_name):
            missing_scripts.append(script_name)
    
    if missing_scripts:
        logger.error(f"❌ Missing required scripts: {', '.join(missing_scripts)}")
        sys.exit(1)
    
    # Step 0: Database setup (replaces manual database creation logic)
    logger.info("=" * 60)
    logger.info("STEP 0: DATABASE SETUP AND CONFIGURATION")
    logger.info("=" * 60)
    
    setup_success = run_script('setup-database.py', 'Database setup and configuration')
    if not setup_success:
        logger.error("❌ Database setup failed. Stopping pipeline.")
        logger.error("💡 Please check your database credentials and connectivity.")
        sys.exit(1)
    
    logger.info("✅ Database is ready for data import!")
    
    # Step 1: S3 to RDS import
    logger.info("=" * 60)
    logger.info("STEP 1: S3 TO RDS IMPORT")
    logger.info("=" * 60)

    s3_success = run_script('s3-to-rds.py', 'S3 to RDS import')
    if not s3_success:
        logger.warning("⚠️ S3 to RDS import failed, but continuing with local CSV import")

    # Step 2: Local CSV to RDS import
    logger.info("=" * 60)
    logger.info("STEP 2: LOCAL CSV TO RDS IMPORT")
    logger.info("=" * 60)

    csv_success = run_script('csv-to-rds-via-s3.py', 'Local CSV to RDS import')
    if not csv_success:
        logger.warning("⚠️ Local CSV to RDS import had issues")

    # Final summary
    logger.info("=" * 60)
    logger.info("📊 PIPELINE EXECUTION SUMMARY")
    logger.info("=" * 60)
    
    logger.info("✅ Database setup: PASSED")
    logger.info(f"{'✅' if s3_success else '❌'} S3 to RDS import: {'PASSED' if s3_success else 'FAILED'}")
    logger.info(f"{'✅' if csv_success else '❌'} Local CSV to RDS import: {'PASSED' if csv_success else 'FAILED'}")

    if s3_success and csv_success:
        logger.info("🎉 All pipeline steps completed successfully!")
        return 0
    elif csv_success:
        logger.info("🎯 Pipeline completed with S3 import issues (local CSV import successful)")
        return 0
    else:
        logger.error("❌ Pipeline completed with errors")
        return 1

if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)
