#!/usr/bin/env python3
"""
Meltano Pipeline Runner for RDS MySQL → BigQuery ETL
Automated data pipeline using Meltano tap-mysql and target-bigquery

Usage:
    python run-pipeline-meltano.py                    # Run full pipeline
    python run-pipeline-meltano.py --dry-run          # Show what would run
    python run-pipeline-meltano.py --table table_name # Run specific table
    python run-pipeline-meltano.py --check-config     # Validate configuration
"""

import os
import sys
import json
import subprocess
import logging
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Any, Optional
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('rds-to-bq-meltano.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class MeltanoPipelineRunner:
    """Meltano pipeline runner for RDS MySQL to BigQuery ETL"""
    
    def __init__(self):
        self.start_time = datetime.now()
        self.meltano_dir = Path(__file__).parent
        self.extractor = "tap-mysql"
        self.loader = "target-bigquery"
        logger.info(f"🚀 Starting Meltano Pipeline at {self.start_time}")
        logger.info(f"📁 Meltano directory: {self.meltano_dir}")
    
    def check_environment(self):
        """Check if Meltano environment is properly set up"""
        logger.info("🔍 Checking Meltano environment...")
        
        # Check if meltano.yml exists
        meltano_yml = self.meltano_dir / "meltano.yml"
        if not meltano_yml.exists():
            logger.error("❌ meltano.yml not found")
            return False
        
        # Check if meltano command is available
        try:
            result = subprocess.run(['meltano', '--version'], 
                                  capture_output=True, text=True, 
                                  cwd=self.meltano_dir)
            if result.returncode == 0:
                logger.info(f"✅ Meltano version: {result.stdout.strip()}")
            else:
                logger.error("❌ Meltano command not found")
                return False
        except FileNotFoundError:
            logger.error("❌ Meltano not installed or not in PATH")
            return False
        
        return True
    
    def check_configuration(self):
        """Validate Meltano configuration"""
        logger.info("🔧 Checking Meltano configuration...")
        
        try:
            # Check extractor configuration
            result = subprocess.run(['meltano', 'config', self.extractor], 
                                  capture_output=True, text=True, 
                                  cwd=self.meltano_dir)
            if result.returncode != 0:
                logger.error(f"❌ Extractor {self.extractor} configuration error:")
                logger.error(result.stderr)
                return False
            
            logger.info(f"✅ {self.extractor} configuration valid")
            
            # Check loader configuration
            result = subprocess.run(['meltano', 'config', self.loader], 
                                  capture_output=True, text=True, 
                                  cwd=self.meltano_dir)
            if result.returncode != 0:
                logger.error(f"❌ Loader {self.loader} configuration error:")
                logger.error(result.stderr)
                return False
            
            logger.info(f"✅ {self.loader} configuration valid")
            return True
            
        except Exception as e:
            logger.error(f"❌ Configuration check failed: {e}")
            return False
    
    def list_available_streams(self):
        """List available streams from the source"""
        logger.info("📋 Discovering available streams...")
        
        try:
            result = subprocess.run(['meltano', 'invoke', self.extractor, '--discover'], 
                                  capture_output=True, text=True, 
                                  cwd=self.meltano_dir)
            
            if result.returncode == 0:
                # Parse catalog to extract stream names
                catalog = json.loads(result.stdout)
                streams = [stream['tap_stream_id'] for stream in catalog.get('streams', [])]
                logger.info(f"📊 Found {len(streams)} streams:")
                for stream in streams:
                    logger.info(f"   • {stream}")
                return streams
            else:
                logger.warning(f"⚠️ Discovery failed: {result.stderr}")
                return []
                
        except Exception as e:
            logger.error(f"❌ Stream discovery failed: {e}")
            return []
    
    def run_post_hook_cleanup(self):
        """
        Manually execute the post-hook cleanup since Meltano hooks aren't triggering automatically.
        This is a workaround for Meltano v3.7.8 hook execution issues.
        """
        try:
            logger.info("Executing post-hook cleanup manually...")
            
            # Change to the correct directory where the post-hook script is located
            post_hook_script = os.path.join(self.meltano_dir, "meltano-post-hook.py")
            
            if not os.path.exists(post_hook_script):
                logger.error(f"Post-hook script not found at: {post_hook_script}")
                return False
            
            # Execute the post-hook script
            result = subprocess.run(
                ["python", "meltano-post-hook.py"],
                cwd=self.meltano_dir,
                capture_output=True,
                text=True,
                timeout=300  # 5 minute timeout for cleanup
            )
            
            if result.returncode == 0:
                logger.info("✅ Post-hook cleanup completed successfully")
                if result.stdout:
                    logger.info(f"Cleanup output: {result.stdout}")
                return True
            else:
                logger.error(f"❌ Post-hook cleanup failed with exit code: {result.returncode}")
                if result.stderr:
                    logger.error(f"Cleanup error: {result.stderr}")
                return False
                
        except subprocess.TimeoutExpired:
            logger.error("❌ Post-hook cleanup timed out after 5 minutes")
            return False
        except Exception as e:
            logger.error(f"❌ Error executing post-hook cleanup: {str(e)}")
            return False

    def run_elt_pipeline(self, specific_table=None, dry_run=False):
        """
        Main method to run the ELT pipeline
        """
        logger.info("="*60)
        logger.info("🔄 MELTANO ETL PIPELINE: RDS MYSQL → BIGQUERY")
        logger.info("="*60)
        
        if not self.check_environment():
            return False
        
        if not self.check_configuration():
            return False
        
        # Prepare run command
        run_cmd = ['meltano', 'run', f'{self.extractor}', f'{self.loader}']
        
        if specific_table:
            logger.info(f"🎯 Running pipeline for specific table: {specific_table}")
            # Add table selection if supported by your tap
            run_cmd.extend(['--select', specific_table])
        else:
            logger.info("🎯 Running full pipeline for all configured tables")
        
        if dry_run:
            logger.info("🧪 DRY RUN - Command that would be executed:")
            logger.info(f"   Command: {' '.join(run_cmd)}")
            logger.info(f"   Working directory: {self.meltano_dir}")
            return True
        
        logger.info("🚀 Starting ETL execution...")
        logger.info(f"📍 Command: {' '.join(run_cmd)}")
        
        try:
            # Run the pipeline
            result = subprocess.run(run_cmd, 
                                  cwd=self.meltano_dir,
                                  text=True,
                                  capture_output=False,  # Show output in real-time
                                  )
            
            if result.returncode == 0:
                end_time = datetime.now()
                duration = end_time - self.start_time
                logger.info("="*60)
                logger.info("🎉 PIPELINE COMPLETED SUCCESSFULLY")
                logger.info(f"   Start time: {self.start_time}")
                logger.info(f"   End time: {end_time}")
                logger.info(f"   Duration: {duration}")
                logger.info("="*60)
                
                # Manual post-hook execution since Meltano hooks aren't firing
                logger.info("🔧 Manually triggering post-hook cleanup...")
                return self.run_post_hook_cleanup()
            else:
                logger.error("❌ Pipeline execution failed")
                logger.error(f"   Exit code: {result.returncode}")
                return False
                
        except Exception as e:
            logger.error(f"❌ Pipeline execution error: {e}")
            return False
    
    def show_status(self):
        """Show pipeline status and recent runs"""
        logger.info("📊 Checking pipeline status...")
        
        try:
            # Show Meltano environment
            result = subprocess.run(['meltano', 'environment', 'list'], 
                                  capture_output=True, text=True, 
                                  cwd=self.meltano_dir)
            if result.returncode == 0:
                logger.info("🌍 Available environments:")
                for line in result.stdout.strip().split('\n'):
                    if line.strip():
                        logger.info(f"   {line}")
        
            # Show installed plugins
            result = subprocess.run(['meltano', 'invoke', '--list-commands'], 
                                  capture_output=True, text=True, 
                                  cwd=self.meltano_dir)
            if result.returncode == 0:
                logger.info("🔌 Installed plugins:")
                logger.info(f"   Extractor: {self.extractor}")
                logger.info(f"   Loader: {self.loader}")
            
        except Exception as e:
            logger.warning(f"⚠️ Status check warning: {e}")
    
    def test_connection(self):
        """Test connection to source and target"""
        logger.info("🔗 Testing connections...")
        
        # Test MySQL connection
        logger.info(f"🔍 Testing {self.extractor} connection...")
        try:
            result = subprocess.run(['meltano', 'invoke', self.extractor, '--test'], 
                                  capture_output=True, text=True, 
                                  cwd=self.meltano_dir)
            if result.returncode == 0:
                logger.info(f"✅ {self.extractor} connection successful")
            else:
                logger.warning(f"⚠️ {self.extractor} connection test failed: {result.stderr}")
        except Exception as e:
            logger.warning(f"⚠️ {self.extractor} connection test error: {e}")
        
        # Test BigQuery connection (if supported)
        logger.info(f"🔍 Testing {self.loader} connection...")
        try:
            result = subprocess.run(['meltano', 'invoke', self.loader, '--test'], 
                                  capture_output=True, text=True, 
                                  cwd=self.meltano_dir)
            if result.returncode == 0:
                logger.info(f"✅ {self.loader} connection successful")
            else:
                logger.warning(f"⚠️ {self.loader} connection test failed: {result.stderr}")
        except Exception as e:
            logger.warning(f"⚠️ {self.loader} connection test error: {e}")

def main():
    """Main execution function"""
    runner = MeltanoPipelineRunner()
    
    # Parse command line arguments
    dry_run = '--dry-run' in sys.argv
    check_config = '--check-config' in sys.argv
    show_help = '--help' in sys.argv or '-h' in sys.argv
    
    # Extract table name if provided
    specific_table = None
    if '--table' in sys.argv:
        try:
            table_index = sys.argv.index('--table') + 1
            if table_index < len(sys.argv):
                specific_table = sys.argv[table_index]
        except (ValueError, IndexError):
            logger.error("❌ --table requires a table name")
            sys.exit(1)
    
    if show_help:
        print(__doc__)
        sys.exit(0)
    
    if check_config:
        logger.info("🔧 Configuration check mode")
        runner.show_status()
        runner.test_connection()
        success = runner.check_configuration()
        sys.exit(0 if success else 1)
    
    # Run the pipeline
    success = runner.run_elt_pipeline(specific_table=specific_table, dry_run=dry_run)
    
    if success:
        logger.info("🎉 Meltano pipeline completed successfully!")
        sys.exit(0)
    else:
        logger.error("❌ Meltano pipeline failed!")
        sys.exit(1)

if __name__ == "__main__":
    main()
