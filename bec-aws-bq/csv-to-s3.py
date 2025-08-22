#!/usr/bin/env python3
"""
Move CSV to S3 
1. Scans CSV from local file system bec-aws\csv-source-file
2. Uploads CSV files to S3 bucket 'bec-bucket-aws/s3-to-rds'
3. If CSV file successfully imported to S3 bucket then move CSV file to bec-aws\csv-imported-to-rds
4. along with successful above then clear CSV file from bec-aws\csv-source-file
"""
import os
import sys
import boto3
import shutil
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
        
        # S3 configuration
        self.s3_bucket = os.getenv('S3_BUCKET', 'bec-bucket-aws')
        self.s3_staging_prefix = os.getenv('S3_SOURCE_PREFIX', 's3-to-rds/')  # Upload to s3-to-rds folder

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
        """Upload all CSV files to S3 and move successful uploads to imported folder"""
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
                # If upload successful, move file to imported folder
                if self.move_file_to_imported(file_info['filename']):
                    successful_uploads += 1
                    logger.info(f"✅ {file_info['filename']} uploaded to S3 and moved to imported folder")
                else:
                    logger.warning(f"⚠️ {file_info['filename']} uploaded to S3 but failed to move to imported folder")
                    successful_uploads += 1  # Still count as successful upload
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

    def run_full_pipeline(self):
        try:
            logger.info("="*60)
            logger.info("🚀 STARTING CSV TO S3 PIPELINE")
            logger.info("="*60)
            logger.info(f"📂 Source folder: {self.source_folder}")
            logger.info(f"📂 Imported folder: {self.imported_folder}")
            logger.info(f"☁️ S3 destination: s3://{self.s3_bucket}/{self.s3_staging_prefix}")
            
            # Upload CSV files to S3 and move successful uploads to imported folder
            logger.info("\n📤 STEP 1: Uploading CSV files to S3 and moving to imported folder...")
            if not self.upload_all_csv_files():
                logger.error("❌ Failed to upload files to S3")
                return False
            
            logger.info("="*60)
            logger.info(f"✅ CSV files uploaded to S3")
            logger.info(f"✅ Successfully uploaded files moved to imported folder")
            logger.info(f"✅ Source files cleaned from csv-source-file folder")

            return True
            
        except Exception as e:
            logger.error(f"❌ Pipeline failed: {str(e)}")
            return False

def main():
    """Main function to run the CSV to S3 pipeline"""
    
    # Define folder paths
    script_dir = os.path.dirname(os.path.abspath(__file__))
    source_folder = os.path.join(script_dir, 'csv-source-file')
    imported_folder = os.path.join(script_dir, 'csv-imported-to-rds')
    
    # Validate environment - only need S3 configuration
    s3_bucket = os.getenv('S3_BUCKET')
    aws_access_key = os.getenv('AWS_ACCESS_KEY_ID')
    aws_secret_key = os.getenv('AWS_SECRET_ACCESS_KEY')
    aws_region = os.getenv('AWS_DEFAULT_REGION')
    
    required_vars = {
        'S3 Bucket': s3_bucket,
        'AWS Access Key': aws_access_key,
        'AWS Secret Key': aws_secret_key,
        'AWS Region': aws_region
    }
    
    missing_vars = [name for name, value in required_vars.items() if not value]
    
    if missing_vars:
        logger.error("❌ Missing required environment variables:")
        for var in missing_vars:
            logger.error(f"   {var}")
        logger.error("💡 Please set these in your .env file:")
        logger.error("   S3_BUCKET, AWS_ACCESS_KEY_ID,")
        logger.error("   AWS_SECRET_ACCESS_KEY, AWS_DEFAULT_REGION")
        sys.exit(1)
    
    # Run the pipeline
    uploader = CSVToS3Uploader(source_folder, imported_folder)
    success = uploader.run_full_pipeline()
    
    if not success:
        logger.error("❌ Pipeline failed!")
        sys.exit(1)
    
    logger.info("🎉 All done! CSV to S3 pipeline completed successfully.")

if __name__ == "__main__":
    main()
