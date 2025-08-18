#!/usr/bin/env python3
"""
S3 to RDS MySQL loader with automatic file management
1. Downloads CSV files from S3 bucket 'bec-bucket-aws/s3-to-rds/'
2. Loads them into RDS MySQL database
3. After successful import, moves files to 's3-imported-to-rds/' folder in S3
4. Provides clear workflow for managing S3 to RDS imports
"""

import os
import sys
import pandas as pd
from sqlalchemy import create_engine, text
import pymysql
import boto3
from urllib.parse import quote_plus
from dotenv import load_dotenv
import logging
import tempfile
from botocore.exceptions import ClientError, NoCredentialsError

# Load environment variables
load_dotenv()

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class S3ToRDSLoader:
    def __init__(self, 
                 s3_bucket,
                 s3_source_prefix,
                 s3_imported_prefix,
                 mysql_host, 
                 mysql_database, 
                 mysql_username, 
                 mysql_password,
                 mysql_port=3306):
        """
        Initialize the S3 to RDS loader
        """
        self.s3_bucket = s3_bucket
        self.s3_source_prefix = s3_source_prefix
        self.s3_imported_prefix = s3_imported_prefix
        self.mysql_host = mysql_host
        self.mysql_database = mysql_database
        self.mysql_username = mysql_username
        self.mysql_password = mysql_password
        self.mysql_port = mysql_port
        
        # Initialize S3 client
        try:
            self.s3_client = boto3.client('s3')
            logger.info("✅ S3 client initialized successfully")
        except NoCredentialsError:
            logger.error("❌ AWS credentials not found. Please configure AWS credentials.")
            sys.exit(1)
        
        # Create database engine
        self.engine = None
        self.setup_database()
    
    def setup_database(self):
        """Create connection engine to existing database"""
        try:
            # Create engine with the specific database (assuming it already exists)
            self.engine = create_engine(
                f"mysql+pymysql://{self.mysql_username}:{quote_plus(self.mysql_password)}@{self.mysql_host}:{self.mysql_port}/{self.mysql_database}",
                echo=False
            )
            
            # Test the connection
            with self.engine.connect() as connection:
                result = connection.execute(text("SELECT 1"))
                logger.info(f"✅ Connected to database '{self.mysql_database}' successfully")
                
        except Exception as e:
            logger.error(f"❌ Failed to connect to database '{self.mysql_database}': {str(e)}")
            logger.error("Make sure the database exists and credentials are correct.")
            sys.exit(1)
    
    def list_s3_files(self):
        """List CSV files in the S3 source folder"""
        try:
            response = self.s3_client.list_objects_v2(
                Bucket=self.s3_bucket,
                Prefix=self.s3_source_prefix
            )
            
            csv_files = []
            if 'Contents' in response:
                for obj in response['Contents']:
                    key = obj['Key']
                    if key.endswith('.csv') and key != self.s3_source_prefix:
                        csv_files.append(key)
            
            logger.info(f"📁 Found {len(csv_files)} CSV files in s3://{self.s3_bucket}/{self.s3_source_prefix}")
            return csv_files
            
        except ClientError as e:
            logger.error(f"❌ Failed to list S3 files: {str(e)}")
            return []
    
    def download_csv_from_s3(self, s3_key):
        """Download CSV file from S3 to temporary file"""
        try:
            temp_file = tempfile.NamedTemporaryFile(delete=False, suffix='.csv')
            self.s3_client.download_file(self.s3_bucket, s3_key, temp_file.name)
            logger.info(f"📥 Downloaded {s3_key} to temporary file")
            return temp_file.name
        except ClientError as e:
            logger.error(f"❌ Failed to download {s3_key}: {str(e)}")
            return None
    
    def move_s3_file_to_imported(self, s3_key):
        """Move file from source to imported folder in S3"""
        try:
            # Extract filename from the key
            filename = os.path.basename(s3_key)
            new_key = f"{self.s3_imported_prefix}{filename}"
            
            # Copy file to new location
            copy_source = {'Bucket': self.s3_bucket, 'Key': s3_key}
            self.s3_client.copy_object(CopySource=copy_source, Bucket=self.s3_bucket, Key=new_key)
            
            # Delete original file
            self.s3_client.delete_object(Bucket=self.s3_bucket, Key=s3_key)
            
            logger.info(f"📁 Moved {s3_key} to {new_key}")
            return True
            
        except ClientError as e:
            logger.error(f"❌ Failed to move {s3_key}: {str(e)}")
            return False
    
    def load_csv_to_mysql(self, csv_file_path, table_name):
        """Load CSV file into MySQL table"""
        try:
            # Read CSV file
            df = pd.read_csv(csv_file_path)
            logger.info(f"📊 Loaded CSV with {len(df)} rows and {len(df.columns)} columns")
            
            # Load to MySQL
            df.to_sql(
                name=table_name,
                con=self.engine,
                if_exists='replace',  # Replace table if it exists
                index=False,
                method='multi',
                chunksize=1000
            )
            
            logger.info(f"✅ Successfully imported {len(df)} rows into table '{table_name}'")
            return True
            
        except Exception as e:
            logger.error(f"❌ Failed to load CSV to MySQL: {str(e)}")
            return False
    
    def process_s3_file(self, s3_key):
        """Process a single S3 file: download, import, and move"""
        # Extract table name from filename (remove .csv extension)
        filename = os.path.basename(s3_key)
        table_name = os.path.splitext(filename)[0]
        
        logger.info(f"🔄 Processing {filename} -> table '{table_name}'")
        
        # Download file from S3
        temp_file = self.download_csv_from_s3(s3_key)
        if not temp_file:
            return False
        
        try:
            # Load CSV to MySQL
            success = self.load_csv_to_mysql(temp_file, table_name)
            
            if success:
                # Move file to imported folder in S3
                if self.move_s3_file_to_imported(s3_key):
                    logger.info(f"✅ Successfully processed and moved {filename}")
                    return True
                else:
                    logger.warning(f"⚠️ File imported but failed to move {filename}")
                    return False
            else:
                logger.error(f"❌ Failed to import {filename}")
                return False
                
        finally:
            # Clean up temporary file
            if os.path.exists(temp_file):
                os.unlink(temp_file)
    
    def process_all_s3_files(self):
        """Process all CSV files from S3 source folder"""
        logger.info(f"🚀 Starting S3 to RDS import process...")
        logger.info(f"📂 Source: s3://{self.s3_bucket}/{self.s3_source_prefix}")
        logger.info(f"📂 Target: {self.mysql_host}/{self.mysql_database}")
        logger.info(f"📂 Imported: s3://{self.s3_bucket}/{self.s3_imported_prefix}")
        
        # Get list of CSV files
        csv_files = self.list_s3_files()
        
        if not csv_files:
            logger.info("📭 No CSV files found to process")
            return
        
        # Process each file
        successful_imports = 0
        failed_imports = 0
        
        for s3_key in csv_files:
            try:
                if self.process_s3_file(s3_key):
                    successful_imports += 1
                else:
                    failed_imports += 1
                    
            except Exception as e:
                logger.error(f"❌ Error processing {s3_key}: {str(e)}")
                failed_imports += 1
        
        # Summary
        logger.info(f"\n📊 S3 to RDS Import Summary:")
        logger.info(f"✅ Successful imports and moves: {successful_imports}")
        logger.info(f"❌ Failed imports: {failed_imports}")
        logger.info(f"📁 Files moved to: s3://{self.s3_bucket}/{self.s3_imported_prefix}")

def main():
    """Main function to run S3 to RDS import"""
    # Get configuration from environment variables
    s3_bucket = os.getenv('S3_BUCKET', 'bec-bucket-aws')
    s3_source_prefix = 's3-to-rds/'
    s3_imported_prefix = 's3-imported-to-rds/'
    
    mysql_host = os.getenv('MYSQL_HOST')
    mysql_database = os.getenv('MYSQL_DATABASE', 'bec-db-aws')
    mysql_username = os.getenv('MYSQL_USERNAME')
    mysql_password = os.getenv('MYSQL_PASSWORD')
    mysql_port = int(os.getenv('MYSQL_PORT', '3306'))
    
    # Validate configuration
    if not all([mysql_host, mysql_username, mysql_password]):
        logger.error("❌ Missing required environment variables. Please check your .env file.")
        sys.exit(1)
    
    # Create loader and process files
    loader = S3ToRDSLoader(
        s3_bucket=s3_bucket,
        s3_source_prefix=s3_source_prefix,
        s3_imported_prefix=s3_imported_prefix,
        mysql_host=mysql_host,
        mysql_database=mysql_database,
        mysql_username=mysql_username,
        mysql_password=mysql_password,
        mysql_port=mysql_port
    )
    
    loader.process_all_s3_files()

if __name__ == "__main__":
    main()
