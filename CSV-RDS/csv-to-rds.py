#!/usr/bin/env python3
"""
CSV to RDS MySQL loader with automatic database setup and file management
1. Automatically ensures database exists and is accessible
2. Reads CSV files from 'csv_to_rds/' folder for processing
3. After successful import, moves files to 'csv_imported_to_rds/' folder
4. Provides clear workflow for managing CSV imports
"""

import os
import sys
import pandas as pd
from sqlalchemy import create_engine, text
import pymysql
from urllib.parse import quote_plus
from dotenv import load_dotenv
import logging
import shutil

# Load environment variables
load_dotenv()

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class CSVToRDSLoader:
    def __init__(self, 
                 source_folder,
                 imported_folder,
                 mysql_host, 
                 mysql_database, 
                 mysql_username, 
                 mysql_password,
                 mysql_port=3306):
        """
        Initialize the CSV to RDS loader with source and destination folders
        """
        self.source_folder = source_folder
        self.imported_folder = imported_folder
        self.mysql_host = mysql_host
        self.mysql_database = mysql_database
        self.mysql_username = mysql_username
        self.mysql_password = mysql_password
        self.mysql_port = mysql_port
        
        # Ensure folders exist
        os.makedirs(self.source_folder, exist_ok=True)
        os.makedirs(self.imported_folder, exist_ok=True)
        
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
    
    def get_csv_files(self):
        """Get list of CSV files in the source folder"""
        csv_files = []
        
        if not os.path.exists(self.source_folder):
            logger.warning(f"📁 Source folder '{self.source_folder}' does not exist")
            return csv_files
        
        for filename in os.listdir(self.source_folder):
            if filename.lower().endswith('.csv'):
                csv_files.append(filename)
        
        logger.info(f"📁 Found {len(csv_files)} CSV files in '{self.source_folder}'")
        return csv_files
    
    def move_file_to_imported(self, filename):
        """Move file from source to imported folder"""
        source_path = os.path.join(self.source_folder, filename)
        imported_path = os.path.join(self.imported_folder, filename)
        
        try:
            shutil.move(source_path, imported_path)
            logger.info(f"📁 Moved {filename} to imported folder")
            return True
        except Exception as e:
            logger.error(f"❌ Failed to move {filename}: {str(e)}")
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
    
    def process_csv_file(self, filename):
        """Process a single CSV file: load to MySQL and move to imported folder"""
        # Remove .csv extension to get table name
        table_name = os.path.splitext(filename)[0]
        csv_file_path = os.path.join(self.source_folder, filename)
        
        logger.info(f"🔄 Processing {filename} -> table '{table_name}'")
        
        # Load CSV to MySQL
        success = self.load_csv_to_mysql(csv_file_path, table_name)
        
        if success:
            # Move file to imported folder
            if self.move_file_to_imported(filename):
                logger.info(f"✅ Successfully processed and moved {filename}")
                return True
            else:
                logger.warning(f"⚠️ File imported but failed to move {filename}")
                return False
        else:
            logger.error(f"❌ Failed to import {filename}")
            return False
    
    def process_all_files(self):
        """Process all CSV files in the source folder"""
        logger.info(f"🚀 Starting CSV to RDS import process...")
        logger.info(f"📂 Source folder: {os.path.abspath(self.source_folder)}")
        logger.info(f"📂 Target database: {self.mysql_host}/{self.mysql_database}")
        logger.info(f"📂 Imported folder: {os.path.abspath(self.imported_folder)}")
        
        # Get list of CSV files
        csv_files = self.get_csv_files()
        
        if not csv_files:
            logger.info("📭 No CSV files found to process")
            return
        
        # Process each file
        successful_imports = 0
        failed_imports = 0
        
        for filename in csv_files:
            try:
                if self.process_csv_file(filename):
                    successful_imports += 1
                else:
                    failed_imports += 1
                    
            except Exception as e:
                logger.error(f"❌ Error processing {filename}: {str(e)}")
                failed_imports += 1
        
        # Summary
        logger.info(f"\n📊 CSV to RDS Import Summary:")
        logger.info(f"✅ Successful imports and moves: {successful_imports}")
        logger.info(f"❌ Failed imports: {failed_imports}")
        logger.info(f"📁 Files moved to: {os.path.abspath(self.imported_folder)}")

def main():
    """Main function to run CSV to RDS import"""
    # Get configuration from environment variables
    source_folder = './csv-to-rds'
    imported_folder = './csv-imported-to-rds'
    
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
    loader = CSVToRDSLoader(
        source_folder=source_folder,
        imported_folder=imported_folder,
        mysql_host=mysql_host,
        mysql_database=mysql_database,
        mysql_username=mysql_username,
        mysql_password=mysql_password,
        mysql_port=mysql_port
    )
    
    loader.process_all_files()

if __name__ == "__main__":
    main()
