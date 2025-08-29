#!/usr/bin/env python3
"""
Meltano Supabase to BigQuery Transfer Script
High-performance PostgreSQL connection bypass REST API limitations

This script provides a wrapper around the Meltano ELT pipeline for
integration with Dagster and other orchestration tools.
"""

import os
import subprocess
import logging
import sys
from datetime import datetime
from dotenv import load_dotenv

def setup_logging():
    """Setup logging configuration"""
    log_filename = f"meltano_supabase_bq_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_filename),
            logging.StreamHandler()
        ]
    )
    return logging.getLogger(__name__)

def run_meltano_pipeline():
    """Execute the Meltano Supabase to BigQuery pipeline"""
    logger = setup_logging()
    
    logger.info("="*70)
    logger.info("🚀 STARTING MELTANO SUPABASE TO BIGQUERY TRANSFER")
    logger.info("="*70)
    
    try:
        # Load environment variables
        load_dotenv()
        logger.info("✅ Environment variables loaded")
        
        # Set Meltano-specific environment variables explicitly
        os.environ["TARGET_BIGQUERY_PROJECT"] = "dsai-468212"
        os.environ["TARGET_BIGQUERY_DATASET"] = "olist_data_warehouse"
        os.environ["TARGET_BIGQUERY_SUPABASE_PROJECT"] = "dsai-468212"
        os.environ["TARGET_BIGQUERY_SUPABASE_DATASET"] = "olist_data_warehouse"
        logger.info("🔧 BigQuery project and dataset configured")
        
        # Verify Meltano is available
        logger.info("🔍 Checking Meltano availability...")
        check_result = subprocess.run(['meltano', '--version'], 
                                    capture_output=True, text=True)
        if check_result.returncode != 0:
            logger.error("❌ Meltano not found! Please ensure conda environment 'bec' is activated")
            return False
        
        logger.info(f"✅ Meltano version: {check_result.stdout.strip()}")
        
        # Run the Meltano ELT pipeline
        logger.info("🔄 Executing Meltano supabase-to-bigquery pipeline...")
        logger.info("💡 Using PostgreSQL direct connection for optimal performance")
        
        # Execute the pipeline
        result = subprocess.run([
            'meltano', 'elt', 'tap-postgres', 'target-bigquery-supabase'
        ], capture_output=True, text=True)
        
        if result.returncode == 0:
            logger.info("✅ Meltano pipeline completed successfully!")
            logger.info("📊 Data transfer from Supabase to BigQuery complete")
            
            # Log any output
            if result.stdout:
                logger.info(f"Pipeline output:\n{result.stdout}")
            
            return True
        else:
            logger.error(f"❌ Meltano pipeline failed with return code: {result.returncode}")
            if result.stderr:
                logger.error(f"Error output:\n{result.stderr}")
            if result.stdout:
                logger.error(f"Standard output:\n{result.stdout}")
            return False
            
    except subprocess.CalledProcessError as e:
        logger.error(f"❌ Command execution failed: {e}")
        return False
    except Exception as e:
        logger.error(f"❌ Unexpected error: {e}")
        return False

def main():
    """Main execution function"""
    success = run_meltano_pipeline()
    
    if success:
        print("🎉 Supabase to BigQuery transfer completed successfully!")
        sys.exit(0)
    else:
        print("❌ Supabase to BigQuery transfer failed!")
        sys.exit(1)

if __name__ == "__main__":
    main()
