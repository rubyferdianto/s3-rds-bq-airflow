#!/usr/bin/env python3
"""
Meltano Post-Hook Script
Automatically runs RDS cleanup after successful BigQuery import

This script is designed to be called as a post-hook in Meltano pipeline
to automatically clean up RDS data after successful BigQuery transfer.
"""

import os
import sys
import subprocess
import logging
from pathlib import Path
from datetime import datetime

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - [POST-HOOK] %(message)s',
    handlers=[
        logging.FileHandler('/Applications/RF/NTU/SCTP in DSAI/s3-rds-bq-dagster/bec-meltano/meltano-post-hook.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def run_rds_cleanup():
    """Run RDS cleanup script with safety checks"""
    
    logger.info("=" * 60)
    logger.info("🎣 MELTANO POST-HOOK TRIGGERED!")
    logger.info(f"🕐 Timestamp: {datetime.now()}")
    logger.info(f"🐍 Python executable: {sys.executable}")
    logger.info(f"📁 Working directory: {os.getcwd()}")
    logger.info("=" * 60)
    
    # Get the directory where this script is located
    script_dir = Path(__file__).parent
    cleanup_script = script_dir / "delete-rds-after-load.py"
    
    logger.info(f"📂 Script directory: {script_dir}")
    logger.info(f"🧹 Cleanup script path: {cleanup_script}")
    
    if not cleanup_script.exists():
        logger.error(f"❌ Cleanup script not found: {cleanup_script}")
        return False
    
    logger.info("✅ Cleanup script found!")
    logger.info("🧹 Starting automated RDS cleanup after Meltano pipeline...")
    
    try:
        # First, run verification to ensure BigQuery data is present
        logger.info("🔍 Step 1: Verifying BigQuery data...")
        verify_result = subprocess.run([
            sys.executable, str(cleanup_script), "--verify-only"
        ], capture_output=True, text=True)
        
        if verify_result.returncode != 0:
            logger.error("❌ BigQuery verification failed - skipping cleanup")
            logger.error(verify_result.stderr)
            return False
        
        logger.info("✅ BigQuery verification passed")
        
        # Then, run actual cleanup
        logger.info("🗑️ Step 2: Cleaning up RDS data...")
        cleanup_result = subprocess.run([
            sys.executable, str(cleanup_script)
        ], capture_output=True, text=True)
        
        if cleanup_result.returncode == 0:
            logger.info("✅ RDS cleanup completed successfully")
            logger.info(cleanup_result.stdout)
            return True
        else:
            logger.error("❌ RDS cleanup failed")
            logger.error(cleanup_result.stderr)
            return False
            
    except Exception as e:
        logger.error(f"❌ Cleanup execution error: {e}")
        return False

if __name__ == "__main__":
    success = run_rds_cleanup()
    sys.exit(0 if success else 1)
