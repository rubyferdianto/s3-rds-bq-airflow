#!/usr/bin/env python3
"""
S3 to RDS MySQL loader using pandas and direct S3 reading
1. Scans S3 bucket 'bec-bucket-aws/s3-to-rds' for CSV files
2. Reads CSV files directly from S3 using pandas (no local download)
3. Loads data into RDS MySQL using pandas
4. Moves processed files to 's3-imported-to-rds' folder in S3
5. Provides efficient workflow for S3 to RDS imports without local storage
"""

import os
import sys
import boto3
import pymysql
import pandas as pd
from sqlalchemy import create_engine, text
from urllib.parse import quote_plus
from dotenv import load_dotenv
import logging
import time
from datetime import datetime
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
                 s3_dest_prefix,
                 rds_host, 
                 rds_database, 
                 rds_username, 
                 rds_password,
                 rds_port=3306):
        
        self.s3_bucket = s3_bucket
        self.s3_source_prefix = s3_source_prefix
        self.s3_dest_prefix = s3_dest_prefix
        self.rds_host = rds_host
        self.rds_database = rds_database
        self.rds_username = rds_username
        self.rds_password = rds_password
        self.rds_port = rds_port
        
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
        """Create connection engine to RDS MySQL database"""
        try:
            connection_string = f"mysql+pymysql://{self.rds_username}:{quote_plus(self.rds_password)}@{self.rds_host}:{self.rds_port}/{self.rds_database}"
            
            self.engine = create_engine(
                connection_string,
                echo=False,
                pool_pre_ping=True,
                pool_recycle=3600
            )
            
            # Test the connection
            with self.engine.connect() as connection:
                result = connection.execute(text("SELECT 1"))
                logger.info(f"✅ Connected to RDS database '{self.rds_database}' successfully")
                
                # Check MySQL version
                version_result = connection.execute(text("SELECT @@version"))
                version = version_result.scalar()
                logger.info(f"✅ RDS MySQL version: {version}")
                
        except Exception as e:
            logger.error(f"❌ Failed to connect to RDS database '{self.rds_database}': {str(e)}")
            sys.exit(1)
    
    def list_s3_csv_files(self):
        """List all CSV files in S3 source prefix"""
        try:
            logger.info(f"🔍 Scanning S3 for CSV files: s3://{self.s3_bucket}/{self.s3_source_prefix}")
            
            paginator = self.s3_client.get_paginator('list_objects_v2')
            pages = paginator.paginate(Bucket=self.s3_bucket, Prefix=self.s3_source_prefix)
            
            csv_files = []
            for page in pages:
                if 'Contents' in page:
                    for obj in page['Contents']:
                        key = obj['Key']
                        if key.lower().endswith('.csv') and key != self.s3_source_prefix:
                            file_size = obj['Size'] / (1024 * 1024)  # Size in MB
                            csv_files.append({
                                'key': key,
                                'filename': os.path.basename(key),
                                'size_mb': file_size,
                                'last_modified': obj['LastModified']
                            })
            
            logger.info(f"📁 Found {len(csv_files)} CSV file(s) in S3:")
            for file_info in csv_files:
                logger.info(f"  - {file_info['filename']} ({file_info['size_mb']:.1f} MB)")
            
            return csv_files
            
        except ClientError as e:
            logger.error(f"❌ Failed to list S3 files: {str(e)}")
            return []
    
    def create_table_from_csv_structure(self, s3_key, table_name):
        """Create table structure by analyzing CSV headers"""
        try:
            logger.info(f"🔍 Analyzing CSV structure for table creation...")
            
            # Read just the first few rows to determine structure
            response = self.s3_client.get_object(
                Bucket=self.s3_bucket, 
                Key=s3_key,
                Range='bytes=0-8192'  # Read first 8KB to get headers and sample data
            )
            
            # Parse the first few lines
            content = response['Body'].read().decode('utf-8')
            lines = content.split('\n')
            
            if len(lines) < 2:
                logger.error(f"❌ CSV file appears to be empty or invalid")
                return False
            
            # Get headers
            headers = lines[0].strip().split(',')
            headers = [header.strip('"').strip() for header in headers]
            
            logger.info(f"📊 CSV headers: {headers}")
            
            # Create table with TEXT columns (RDS will handle data types automatically)
            with self.engine.begin() as connection:
                # Drop table if exists
                drop_sql = f"DROP TABLE IF EXISTS `{table_name}`"
                connection.execute(text(drop_sql))
                
                # Create table structure
                columns = []
                for header in headers:
                    # Clean column name
                    clean_header = ''.join(c if c.isalnum() or c == '_' else '_' for c in header)
                    if clean_header[0].isdigit():
                        clean_header = f"col_{clean_header}"
                    columns.append(f"`{clean_header}` TEXT")
                
                # Add CREATED_DATE column with automatic current timestamp
                columns.append("`CREATED_DATE` TIMESTAMP DEFAULT CURRENT_TIMESTAMP")
                
                create_sql = f"CREATE TABLE `{table_name}` ({', '.join(columns)})"
                connection.execute(text(create_sql))
                
                logger.info(f"✅ Created table '{table_name}' with {len(columns)} columns (including CREATED_DATE)")
                return True
                
        except Exception as e:
            logger.error(f"❌ Failed to create table structure: {str(e)}")
            return False
    
    def load_csv_from_s3_to_rds(self, s3_key, table_name):
        """Load CSV directly from S3 to RDS MySQL using pandas (no local download)"""
        try:
            start_time = time.time()
            s3_url = f"s3://{self.s3_bucket}/{s3_key}"
            
            logger.info(f"🚀 Loading CSV directly from S3 to RDS MySQL using pandas...")
            logger.info(f"📥 Source: {s3_url}")
            logger.info(f"📊 Target table: {table_name}")
            
            # Read CSV directly from S3 using pandas (no local download!)
            logger.info(f"📊 Reading CSV directly from S3 with pandas...")
            df = pd.read_csv(s3_url)
            
            logger.info(f"📊 CSV contains {len(df)} rows and {len(df.columns)} columns")
            logger.info(f"📊 Columns: {list(df.columns)}")
            
            # Add CREATED_DATE column with current timestamp
            current_timestamp = datetime.now()
            df['CREATED_DATE'] = current_timestamp
            logger.info(f"✅ Added CREATED_DATE column with timestamp: {current_timestamp}")
            
            # Load data into RDS MySQL using pandas
            logger.info(f"⬆️ Loading data into RDS MySQL...")
            df.to_sql(
                name=table_name,
                con=self.engine,
                if_exists='replace',  # Replace table if exists
                index=False,
                method='multi',
                chunksize=1000
            )
            
            elapsed_time = time.time() - start_time
            rate = len(df) / elapsed_time if elapsed_time > 0 else 0
            
            logger.info(f"✅ RDS MySQL import completed!")
            logger.info(f"📊 Loaded {len(df):,} rows in {elapsed_time:.2f} seconds ({rate:.0f} rows/sec)")
            logger.info(f"🗄️ Table '{table_name}' created successfully")
            logger.info(f"💡 Direct S3 → RDS transfer (no local download!)")
            
            return True
                
        except Exception as e:
            logger.error(f"❌ RDS MySQL import failed: {str(e)}")
            return False
    
    def move_s3_file_to_imported(self, source_key):
        """Move file from source to imported folder in S3"""
        try:
            filename = os.path.basename(source_key)
            dest_key = f"{self.s3_dest_prefix}{filename}"
            
            logger.info(f"📋 Moving S3 file to imported folder...")
            logger.info(f"📍 From: s3://{self.s3_bucket}/{source_key}")
            logger.info(f"📍 To: s3://{self.s3_bucket}/{dest_key}")
            
            # Copy file to destination
            copy_source = {'Bucket': self.s3_bucket, 'Key': source_key}
            self.s3_client.copy_object(
                CopySource=copy_source,
                Bucket=self.s3_bucket,
                Key=dest_key
            )
            
            # Delete source file (only the file, not the folder)
            self.s3_client.delete_object(Bucket=self.s3_bucket, Key=source_key)
            
            logger.info(f"✅ S3 file moved successfully")
            return True
            
        except ClientError as e:
            logger.error(f"❌ Failed to move S3 file: {str(e)}")
            return False
    
    def process_s3_file(self, file_info):
        """Process a single S3 CSV file using RDS native S3 integration"""
        s3_key = file_info['key']
        filename = file_info['filename']
        
        # Create table name from filename
        table_name = os.path.splitext(filename)[0]
        table_name = table_name.replace('-', '_').replace(' ', '_').lower()
        table_name = ''.join(c for c in table_name if c.isalnum() or c == '_')
        
        logger.info(f"\n{'='*60}")
        logger.info(f"📄 Processing: {filename} → table '{table_name}'")
        logger.info(f"📊 File size: {file_info['size_mb']:.1f} MB")
        logger.info(f"{'='*60}")
        
        try:
            total_start_time = time.time()
            
            # Step 1: Create table structure
            logger.info(f"🔧 Step 1: Creating table structure...")
            if not self.create_table_from_csv_structure(s3_key, table_name):
                logger.error(f"❌ Failed to create table structure")
                return False
            
            # Step 2: Load data using pandas
            logger.info(f"🚀 Step 2: RDS MySQL import using pandas...")
            if not self.load_csv_from_s3_to_rds(s3_key, table_name):
                logger.error(f"❌ Failed to load data into RDS")
                return False
            
            # Step 3: Move S3 file to imported folder
            logger.info(f"📋 Step 3: Moving S3 file to imported folder...")
            if not self.move_s3_file_to_imported(s3_key):
                logger.warning(f"⚠️ Failed to move S3 file, but data was loaded successfully")
            
            total_time = time.time() - total_start_time
            logger.info(f"🎉 Successfully processed {filename} in {total_time:.2f} seconds!")
            logger.info(f"✅ RDS table: {table_name}")
            logger.info(f"✅ S3 file moved to: s3-imported-to-rds/{filename}")
            
            return True
            
        except Exception as e:
            logger.error(f"❌ Failed to process {filename}: {str(e)}")
            return False
    
    def process_all_s3_files(self):
        """Process all CSV files in S3 using direct pandas reading for RDS import"""
        logger.info(f"🚀 Starting S3 to RDS processing with direct pandas reading")
        logger.info(f"💡 Direct S3 → pandas → RDS: No local downloads!")
        
        # List all CSV files in S3
        csv_files = self.list_s3_csv_files()
        
        if not csv_files:
            logger.info(f"📭 No CSV files found in S3 source folder")
            return True
        
        total_size = sum(file_info['size_mb'] for file_info in csv_files)
        logger.info(f"📊 Total size to process: {total_size:.1f} MB")
        
        successful_imports = 0
        failed_imports = 0
        
        for file_info in csv_files:
            try:
                if self.process_s3_file(file_info):
                    successful_imports += 1
                else:
                    failed_imports += 1
            except Exception as e:
                logger.error(f"❌ Unexpected error processing {file_info['filename']}: {str(e)}")
                failed_imports += 1
        
        logger.info(f"\n{'='*60}")
        logger.info(f"📊 S3 TO RDS PROCESSING SUMMARY")
        logger.info(f"{'='*60}")
        logger.info(f"✅ Successful imports: {successful_imports}")
        logger.info(f"❌ Failed imports: {failed_imports}")
        logger.info(f"📁 Total files processed: {len(csv_files)}")
        logger.info(f"🚀 Method: Direct S3 reading with pandas")
        
        if successful_imports > 0:
            logger.info(f"\n📍 Results:")
            logger.info(f"  🗄️ RDS Database: {successful_imports} tables imported")
            logger.info(f"  ☁️ S3 Imported: s3://{self.s3_bucket}/{self.s3_dest_prefix}")
            logger.info(f"  💡 Efficient direct S3 → RDS transfer!")
        
        return failed_imports == 0

def main():
    """Main function"""
    # Required environment variables - support both RDS_* and MYSQL_* variables
    rds_host = os.getenv('MYSQL_HOST')
    rds_database = os.getenv('MYSQL_DATABASE', 'bec_rds_db')
    rds_username = os.getenv('MYSQL_USERNAME')
    rds_password = os.getenv('MYSQL_PASSWORD')
    rds_port = int(os.getenv('MYSQL_PORT', '3306'))
    
    s3_bucket = os.getenv('S3_BUCKET', 'bec-bucket-aws')
    s3_source_prefix = os.getenv('S3_SOURCE_PREFIX', 's3-to-rds/')
    s3_dest_prefix = os.getenv('S3_DEST_PREFIX', 's3-imported-to-rds/')
    
    if not all([rds_host, rds_username, rds_password]):
        logger.error("❌ Missing required database environment variables")
        logger.error("💡 Required: MYSQL_HOST, MYSQL_USERNAME, MYSQL_PASSWORD")
        sys.exit(1)
    
    logger.info("🚀 Starting S3 to RDS MySQL import with direct pandas reading")
    logger.info(f"🗄️ Database host: {rds_host}")
    logger.info(f"🗄️ Database: {rds_database}")
    logger.info(f"☁️ S3 source: s3://{s3_bucket}/{s3_source_prefix}")
    logger.info(f"☁️ S3 destination: s3://{s3_bucket}/{s3_dest_prefix}")
    logger.info(f"💡 Direct S3 → pandas → RDS: No local downloads required!")
    
    loader = S3ToRDSLoader(
        s3_bucket=s3_bucket,
        s3_source_prefix=s3_source_prefix,
        s3_dest_prefix=s3_dest_prefix,
        rds_host=rds_host,
        rds_database=rds_database,
        rds_username=rds_username,
        rds_password=rds_password,
        rds_port=rds_port
    )
    
    success = loader.process_all_s3_files()
    
    if success:
        logger.info("🎉 S3 to RDS import completed successfully!")
    else:
        logger.error("❌ S3 to RDS import completed with errors")
        sys.exit(1)

if __name__ == "__main__":
    main()
