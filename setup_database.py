#!/usr/bin/env python3
"""
MySQL Database Setup Script
This script connects to MySQL and ensures the database exists for CSV loading
"""

import pymysql
import os
from dotenv import load_dotenv
import logging
import sys

# Load environment variables
load_dotenv()

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def setup_database_for_pipeline():
    """Setup database for the pipeline - can be called from other scripts"""
    
    # Get configuration
    mysql_host = os.getenv('MYSQL_HOST')
    mysql_port = int(os.getenv('MYSQL_PORT', '3306'))
    mysql_username = os.getenv('MYSQL_USERNAME')
    mysql_password = os.getenv('MYSQL_PASSWORD')
    mysql_database = os.getenv('MYSQL_DATABASE')
    
    if not all([mysql_host, mysql_username, mysql_password, mysql_database]):
        logger.error("❌ Missing required environment variables. Please check your .env file.")
        return False
    
    logger.info("🔧 Setting up database for pipeline...")
    logger.info(f"📍 Host: {mysql_host}")
    logger.info(f"👤 Username: {mysql_username}")
    logger.info(f"🗄️ Target Database: {mysql_database}")
    
    try:
        # Connect without specifying a database first
        connection = pymysql.connect(
            host=mysql_host,
            port=mysql_port,
            user=mysql_username,
            password=mysql_password,
            connect_timeout=30
        )
        
        logger.info("✅ Connected to MySQL server successfully")
        
        # Create database if it doesn't exist
        with connection.cursor() as cursor:
            cursor.execute(f"CREATE DATABASE IF NOT EXISTS `{mysql_database}`")
            connection.commit()
        
        logger.info(f"✅ Database '{mysql_database}' is ready")
        
        # Test connection to the specific database
        db_connection = pymysql.connect(
            host=mysql_host,
            port=mysql_port,
            user=mysql_username,
            password=mysql_password,
            database=mysql_database,
            connect_timeout=30
        )
        
        # Show existing tables
        with db_connection.cursor() as cursor:
            cursor.execute("SHOW TABLES")
            tables = cursor.fetchall()
        
        if tables:
            logger.info(f"📊 Found {len(tables)} existing tables in '{mysql_database}':")
            for table in tables:
                logger.info(f"  📋 {table[0]}")
        else:
            logger.info(f"🆕 Database '{mysql_database}' is empty (ready for new tables)")
        
        db_connection.close()
        connection.close()
        
        logger.info("🎉 Database setup complete!")
        return True
        
    except Exception as e:
        logger.error(f"❌ Database setup failed: {str(e)}")
        return False

def main():
    """Main function for standalone execution"""
    logger.info("🚀 Starting database setup...")
    
    success = setup_database_for_pipeline()
    
    if success:
        logger.info("✅ Database setup completed successfully!")
        logger.info("You can now run the CSV import scripts.")
    else:
        logger.error("❌ Database setup failed!")
        sys.exit(1)

if __name__ == "__main__":
    main()
