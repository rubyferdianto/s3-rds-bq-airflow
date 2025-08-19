#!/usr/bin/env python3
"""
CSV to RDS MySQL Pipeline with S3 Integration and Pandas
1. Upload CSV files from csv-to-rds folder to S3 's3-to-rds' 
2. Run s3-to-rds.py for pandas-based import (download, process, import)
3. Move local files to csv-imported-to-rds
4. Delete source files from csv-to-rds
"""

import os
import sys
import subprocess
import boto3
import shutil
import time
from dotenv import load_dotenv
import logging

# Load environment variables
load_dotenv()

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class CSVToS3Uploader:
    def __init__(self, source_folder, imported_folder):
        self.source_folder = source_folder
        self.imported_folder = imported_folder
        
        # S3 configuration - same as s3-to-rds.py
        self.s3_bucket = os.getenv('S3_BUCKET', 'bec-bucket-aws')
        self.s3_staging_prefix = 's3-to-rds/'  # Where s3-to-rds.py looks for files
        
        # Ensure folders exist
        os.makedirs(self.source_folder, exist_ok=True)
        os.makedirs(self.imported_folder, exist_ok=True)
        
        # Initialize S3 client
        self.s3_client = None
        self.setup_s3()
    
    def setup_s3(self):
        """Initialize S3 client"""
        try:
            self.s3_client = boto3.client(
                's3',
                aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
                aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
                region_name=os.getenv('AWS_DEFAULT_REGION', 'ap-southeast-1')
            )
            
            # Test S3 connection
            try:
                self.s3_client.head_bucket(Bucket=self.s3_bucket)
                logger.info(f"✅ S3 bucket '{self.s3_bucket}' is accessible")
            except Exception as e:
                logger.error(f"❌ Cannot access S3 bucket '{self.s3_bucket}': {str(e)}")
                raise
                
        except Exception as e:
            logger.error(f"❌ Failed to initialize S3 client: {str(e)}")
            raise
    
    def find_csv_files(self):
        """Find all CSV files in source folder"""
        csv_files = []
        if not os.path.exists(self.source_folder):
            logger.warning(f"⚠️ Source folder '{self.source_folder}' does not exist")
            return csv_files
        
        for file in os.listdir(self.source_folder):
            if file.lower().endswith('.csv'):
                file_path = os.path.join(self.source_folder, file)
                file_size = os.path.getsize(file_path)
                csv_files.append({
                    'filename': file,
                    'filepath': file_path,
                    'size_bytes': file_size,
                    'size_mb': file_size / (1024 * 1024)
                })
        
        return sorted(csv_files, key=lambda x: x['filename'])
    
    def upload_file_to_s3(self, file_info):
        """Upload a single CSV file to S3"""
        try:
            s3_key = f"{self.s3_staging_prefix}{file_info['filename']}"
            
            logger.info(f"📤 Uploading {file_info['filename']} ({file_info['size_mb']:.2f} MB) to s3://{self.s3_bucket}/{s3_key}")
            
            self.s3_client.upload_file(
                file_info['filepath'],
                self.s3_bucket,
                s3_key
            )
            
            logger.info(f"✅ Successfully uploaded {file_info['filename']}")
            return True
            
        except Exception as e:
            logger.error(f"❌ Failed to upload {file_info['filename']}: {str(e)}")
            return False
    
    def upload_all_csv_files(self):
        """Upload all CSV files to S3"""
        csv_files = self.find_csv_files()
        
        if not csv_files:
            logger.info(f"📭 No CSV files found in '{self.source_folder}'")
            return True
        
        total_size = sum(file_info['size_mb'] for file_info in csv_files)
        logger.info(f"📊 Found {len(csv_files)} CSV files totaling {total_size:.2f} MB")
        
        successful_uploads = 0
        failed_uploads = 0
        
        for file_info in csv_files:
            if self.upload_file_to_s3(file_info):
                successful_uploads += 1
            else:
                failed_uploads += 1
        
        logger.info(f"📊 Upload results: {successful_uploads} successful, {failed_uploads} failed")
        return failed_uploads == 0
    
    def move_file_to_imported(self, filename):
        """Move file from source to imported folder"""
        try:
            source_path = os.path.join(self.source_folder, filename)
            dest_path = os.path.join(self.imported_folder, filename)
            
            # Ensure destination directory exists
            os.makedirs(os.path.dirname(dest_path), exist_ok=True)
            
            shutil.move(source_path, dest_path)
            logger.info(f"📁 Moved {filename} to imported folder")
            return True
            
        except Exception as e:
            logger.error(f"❌ Failed to move {filename}: {str(e)}")
            return False
    
    def run_s3_to_rds_import(self):
        """Run the s3-to-rds.py script"""
        try:
            script_path = os.path.join(os.path.dirname(__file__), 's3-to-rds.py')
            
            if not os.path.exists(script_path):
                logger.error(f"❌ s3-to-rds.py script not found at {script_path}")
                return False
            
            logger.info("🚀 Running s3-to-rds.py for pandas-based import...")
            
            result = subprocess.run(
                [sys.executable, script_path],
                capture_output=True,
                text=True,
                timeout=3600  # 1 hour timeout
            )
            
            if result.returncode == 0:
                logger.info("✅ s3-to-rds.py completed successfully")
                logger.info("📄 Output preview:")
                # Show last few lines of output
                output_lines = result.stdout.strip().split('\n')
                for line in output_lines[-5:]:
                    if line.strip():
                        logger.info(f"   {line}")
                return True
            else:
                logger.error(f"❌ s3-to-rds.py failed with return code {result.returncode}")
                logger.error("📄 Error output:")
                for line in result.stderr.strip().split('\n'):
                    if line.strip():
                        logger.error(f"   {line}")
                return False
                
        except subprocess.TimeoutExpired:
            logger.error("❌ s3-to-rds.py timed out after 1 hour")
            return False
        except Exception as e:
            logger.error(f"❌ Failed to run s3-to-rds.py: {str(e)}")
            return False
    
    def cleanup_source_files(self):
        """Move successfully processed files to imported folder"""
        csv_files = self.find_csv_files()
        
        if not csv_files:
            logger.info("📭 No files to clean up")
            return True
        
        successful_moves = 0
        failed_moves = 0
        
        for file_info in csv_files:
            if self.move_file_to_imported(file_info['filename']):
                successful_moves += 1
            else:
                failed_moves += 1
        
        logger.info(f"📊 Cleanup results: {successful_moves} moved, {failed_moves} failed")
        return failed_moves == 0
    
    def run_full_pipeline(self):
        """Run the complete CSV to RDS pipeline"""
        try:
            logger.info("="*60)
            logger.info("🚀 STARTING CSV TO RDS PIPELINE")
            logger.info("="*60)
            logger.info(f"📂 Source folder: {self.source_folder}")
            logger.info(f"📂 Imported folder: {self.imported_folder}")
            logger.info(f"☁️ S3 staging: s3://{self.s3_bucket}/{self.s3_staging_prefix}")
            logger.info(f"💡 Method: Pandas-based import (reliable)")
            
            # Step 1: Upload CSV files to S3
            logger.info("\n📤 STEP 1: Uploading CSV files to S3...")
            if not self.upload_all_csv_files():
                logger.error("❌ Failed to upload files to S3")
                return False
            
            # Step 2: Run s3-to-rds.py for RDS import
            logger.info("\n🗄️ STEP 2: Running RDS import via pandas...")
            if not self.run_s3_to_rds_import():
                logger.error("❌ RDS import failed")
                return False
            
            # Step 3: Clean up source files
            logger.info("\n🧹 STEP 3: Moving processed files to imported folder...")
            if not self.cleanup_source_files():
                logger.warning("⚠️ Some files failed to move to imported folder")
            
            logger.info("\n🎉 PIPELINE COMPLETED SUCCESSFULLY!")
            logger.info("="*60)
            logger.info(f"✅ CSV files uploaded to S3")
            logger.info(f"✅ Data imported to RDS via pandas")
            logger.info(f"✅ Source files moved to imported folder")
            logger.info(f"💡 Pipeline method: Reliable pandas-based processing")
            
            return True
            
        except Exception as e:
            logger.error(f"❌ Pipeline failed: {str(e)}")
            return False

def main():
    """Main function to run the CSV to RDS pipeline"""
    
    # Define folder paths
    script_dir = os.path.dirname(os.path.abspath(__file__))
    source_folder = os.path.join(script_dir, 'csv-to-rds')
    imported_folder = os.path.join(script_dir, 'csv-imported-to-rds')
    
    # Validate environment - support both RDS_* and MYSQL_* variables
    rds_host = os.getenv('MYSQL_HOST')
    rds_username = os.getenv('MYSQL_USERNAME')
    rds_password = os.getenv('MYSQL_PASSWORD')
    rds_database = os.getenv('MYSQL_DATABASE')
    s3_bucket = os.getenv('S3_BUCKET')
    
    required_vars = {
        'Database Host': rds_host,
        'Database Username': rds_username, 
        'Database Password': rds_password,
        'Database Name': rds_database,
        'S3 Bucket': s3_bucket
    }
    
    missing_vars = [name for name, value in required_vars.items() if not value]
    
    if missing_vars:
        logger.error("❌ Missing required environment variables:")
        for var in missing_vars:
            logger.error(f"   {var}")
        logger.error("💡 Please set these in your .env file:")
        logger.error("   MYSQL_HOST, MYSQL_USERNAME,")
        logger.error("   MYSQL_PASSWORD, MYSQL_DATABASE, S3_BUCKET")
        sys.exit(1)
    
    # Run the pipeline
    uploader = CSVToS3Uploader(source_folder, imported_folder)
    success = uploader.run_full_pipeline()
    
    if not success:
        logger.error("❌ Pipeline failed!")
        sys.exit(1)
    
    logger.info("🎉 All done! CSV to RDS pipeline completed successfully.")

if __name__ == "__main__":
    main()
