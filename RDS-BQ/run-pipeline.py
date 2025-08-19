#!/usr/bin/env python3
"""
RDS MySQL to BigQuery Pipeline using Meltano
Extracts data from AWS RDS MySQL and loads into Google BigQuery
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

def check_environment():
    """Check if required environment variables are set"""
    required_vars = [
        'MYSQL_HOST', 'MYSQL_USERNAME', 'MYSQL_PASSWORD', 'MYSQL_DATABASE',
        'GCP_PROJECT', 'BQ_DATASET', 'BQ_LOCATION'
    ]
    
    missing_vars = [var for var in required_vars if not os.getenv(var)]
    
    if missing_vars:
        logger.error(f"❌ Missing required environment variables: {', '.join(missing_vars)}")
        return False
    
    # Check for Google Cloud authentication
    if not os.getenv('GOOGLE_APPLICATION_CREDENTIALS'):
        logger.warning("⚠️ GOOGLE_APPLICATION_CREDENTIALS not set. Make sure you're authenticated with gcloud CLI")
    
    logger.info("✅ Environment variables check passed")
    return True

def run_meltano_command(command, description):
    """Run a meltano command and return success status"""
    logger.info(f"🚀 {description}")
    
    try:
        result = subprocess.run(
            ['meltano'] + command,
            capture_output=True,
            text=True,
            check=True
        )
        
        logger.info(f"✅ {description} completed successfully")
        if result.stdout:
            logger.info(f"Output: {result.stdout}")
        return True
        
    except subprocess.CalledProcessError as e:
        logger.error(f"❌ {description} failed")
        logger.error(f"Error: {e.stderr}")
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
    
    # Step 1: Install plugins if needed
    logger.info("=" * 70)
    logger.info("STEP 1: INSTALLING MELTANO PLUGINS")
    logger.info("=" * 70)
    
    if not run_meltano_command(['install'], 'Installing Meltano plugins'):
        logger.error("💡 Try running 'meltano add extractor tap-mysql' and 'meltano add loader target-bigquery' manually")
        sys.exit(1)
    
    # Step 2: Test connections
    logger.info("=" * 70)
    logger.info("STEP 2: TESTING CONNECTIONS")
    logger.info("=" * 70)
    
    if not run_meltano_command(['invoke', 'tap-mysql', '--test'], 'Testing MySQL connection'):
        logger.error("💡 Check your MySQL credentials and connectivity")
        sys.exit(1)
    
    # Step 3: Run the pipeline
    logger.info("=" * 70)
    logger.info("STEP 3: RUNNING DATA PIPELINE")
    logger.info("=" * 70)
    
    if not run_meltano_command(['run', 'tap-mysql', 'target-bigquery'], 'Running RDS to BigQuery pipeline'):
        logger.error("💡 Check the error messages above for troubleshooting")
        sys.exit(1)
    
    # Success
    logger.info("=" * 70)
    logger.info("🎉 PIPELINE COMPLETED SUCCESSFULLY!")
    logger.info("=" * 70)
    logger.info("💡 Your RDS MySQL data has been loaded into BigQuery!")
    logger.info(f"🔍 Check your BigQuery dataset: {os.getenv('GCP_PROJECT')}.{os.getenv('BQ_DATASET')}")

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        logger.info("\n🛑 Pipeline interrupted by user")
        sys.exit(130)
    except Exception as e:
        logger.error(f"❌ Unexpected error: {str(e)}")
        sys.exit(1)
