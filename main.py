#!/usr/bin/env python3
"""
Main orchestrator for data pipeline
1. Executes check_database to verify database connectivity
2. Executes s3_to_rds to load files from S3 to RDS
3. Executes csv_to_rds to load local CSV files to RDS
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
        ('setup_database.py', 'Database setup'),
        ('check_databases.py', 'Database connectivity check'),
        ('s3_to_rds.py', 'S3 to RDS import'),
        ('csv_to_rds.py', 'Local CSV to RDS import')
    ]
    
    missing_scripts = []
    for script_name, _ in required_scripts:
        if not os.path.exists(script_name):
            missing_scripts.append(script_name)
    
    if missing_scripts:
        logger.error(f"❌ Missing required scripts: {', '.join(missing_scripts)}")
        sys.exit(1)
    
    # Step 0: Setup database first
    logger.info("\n" + "=" * 60)
    logger.info("STEP 0: DATABASE SETUP")
    logger.info("=" * 60)
    
    if not run_script('setup_database.py', 'Database setup'):
        logger.error("❌ Database setup failed. Stopping pipeline.")
        sys.exit(1)
    
    # Step 1: Check database connectivity
    logger.info("\n" + "=" * 60)
    logger.info("STEP 1: DATABASE CONNECTIVITY CHECK")
    logger.info("=" * 60)
    
    if not run_script('check_databases.py', 'Database connectivity check'):
        logger.error("❌ Database check failed. Stopping pipeline.")
        sys.exit(1)
    
    # Step 2: S3 to RDS import
    logger.info("\n" + "=" * 60)
    logger.info("STEP 2: S3 TO RDS IMPORT")
    logger.info("=" * 60)
    
    s3_success = run_script('s3_to_rds.py', 'S3 to RDS import')
    if not s3_success:
        logger.warning("⚠️ S3 to RDS import failed, but continuing with local CSV import")
    
    # Step 3: Local CSV to RDS import
    logger.info("\n" + "=" * 60)
    logger.info("STEP 3: LOCAL CSV TO RDS IMPORT")
    logger.info("=" * 60)
    
    csv_success = run_script('csv_to_rds.py', 'Local CSV to RDS import')
    if not csv_success:
        logger.warning("⚠️ Local CSV to RDS import had issues")
    
    # Final summary
    logger.info("\n" + "=" * 60)
    logger.info("📊 PIPELINE EXECUTION SUMMARY")
    logger.info("=" * 60)
    
    logger.info("✅ Database setup: PASSED")
    logger.info("✅ Database connectivity check: PASSED")
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
