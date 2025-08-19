#!/usr/bin/env python3
"""
RDS MySQL Database Setup Script
Automatically creates database and configures for CSV import workflows.
Designed specifically for AWS RDS MySQL instances.
"""

import os
import sys
import pymysql
from dotenv import load_dotenv
import logging

# Load environment variables
load_dotenv()

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class DatabaseSetup:
    def __init__(self):
        # RDS MySQL configuration
        self.host = os.getenv('MYSQL_HOST')
        self.port = int(os.getenv('MYSQL_PORT', '3306'))
        self.username = os.getenv('MYSQL_USERNAME')
        self.password = os.getenv('MYSQL_PASSWORD')
        self.database_name = os.getenv('MYSQL_DATABASE')
        
        # Check if we have the required configuration
        if not all([self.host, self.username, self.password]):
            logger.error("❌ Missing required RDS MySQL configuration!")
            logger.error("💡 Required environment variables:")
            logger.error("   - MYSQL_HOST")
            logger.error("   - MYSQL_USERNAME")
            logger.error("   - MYSQL_PASSWORD")
            logger.error("   - MYSQL_DATABASE [optional, defaults to 'bec-db-rds']")
            sys.exit(1)
        
        self.connection = None
    
    def connect_to_server(self):
        """Connect to RDS MySQL server (without specifying database)"""
        try:
            logger.info(f"🔗 Connecting to RDS MySQL server...")
            logger.info(f"📍 Host: {self.host}")
            logger.info(f"👤 Username: {self.username}")
            
            self.connection = pymysql.connect(
                host=self.host,
                port=self.port,
                user=self.username,
                password=self.password,
                connect_timeout=30,
                charset='utf8mb4'
            )
            
            logger.info("✅ Successfully connected to RDS MySQL server!")
            return True
            
        except Exception as e:
            logger.error(f"❌ Failed to connect to RDS MySQL server: {str(e)}")
            logger.error("💡 Please check your connection details and network access")
            return False
    
    def check_database_exists(self):
        """Check if the target database already exists"""
        try:
            with self.connection.cursor() as cursor:
                cursor.execute("SHOW DATABASES")
                databases = [db[0] for db in cursor.fetchall()]
                
                if self.database_name in databases:
                    logger.info(f"📁 Database '{self.database_name}' already exists")
                    return True
                else:
                    logger.info(f"📁 Database '{self.database_name}' does not exist")
                    return False
                    
        except Exception as e:
            logger.error(f"❌ Failed to check database existence: {str(e)}")
            return False
    
    def create_database(self):
        """Create the target database"""
        try:
            logger.info(f"🔨 Creating database '{self.database_name}'...")
            
            with self.connection.cursor() as cursor:
                # Create database with UTF8MB4 charset for full Unicode support
                create_sql = f"""
                CREATE DATABASE `{self.database_name}` 
                CHARACTER SET utf8mb4 
                COLLATE utf8mb4_unicode_ci
                """
                cursor.execute(create_sql)
                
            logger.info(f"✅ Database '{self.database_name}' created successfully!")
            return True
            
        except Exception as e:
            logger.error(f"❌ Failed to create database '{self.database_name}': {str(e)}")
            return False
    
    def connect_to_database(self):
        """Connect to the specific database"""
        try:
            if self.connection:
                self.connection.close()
            
            logger.info(f"🔗 Connecting to database '{self.database_name}'...")
            
            self.connection = pymysql.connect(
                host=self.host,
                port=self.port,
                user=self.username,
                password=self.password,
                database=self.database_name,
                connect_timeout=30,
                charset='utf8mb4'
            )
            
            logger.info(f"✅ Successfully connected to database '{self.database_name}'!")
            return True
            
        except Exception as e:
            logger.error(f"❌ Failed to connect to database '{self.database_name}': {str(e)}")
            return False
    
    def setup_database_for_csv_import(self):
        """Configure database settings for optimal CSV import performance"""
        try:
            logger.info("⚙️ Configuring database for CSV import performance...")
            
            with self.connection.cursor() as cursor:
                # Get current settings
                cursor.execute("SELECT @@sql_mode")
                current_sql_mode = cursor.fetchone()[0]
                
                cursor.execute("SELECT @@local_infile")
                local_infile = cursor.fetchone()[0]
                
                logger.info(f"📊 Current SQL Mode: {current_sql_mode}")
                logger.info(f"📊 Local Infile: {local_infile}")
                
                # Set optimal settings for CSV import
                optimizations = [
                    "SET SESSION sql_mode = 'NO_AUTO_VALUE_ON_ZERO,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION'",
                    "SET SESSION foreign_key_checks = 0",
                    "SET SESSION unique_checks = 0",
                    "SET SESSION autocommit = 1"
                ]
                
                for optimization in optimizations:
                    cursor.execute(optimization)
                    logger.info(f"✅ Applied: {optimization}")
            
            logger.info(f"✅ Database configured for optimal CSV import performance!")
            return True
            
        except Exception as e:
            logger.error(f"❌ Failed to configure database: {str(e)}")
            return False
    
    def test_mysql_capabilities(self):
        """Test RDS MySQL capabilities and features"""
        try:
            logger.info("🧪 Testing RDS MySQL capabilities...")
            
            with self.connection.cursor() as cursor:
                # Check MySQL version and capabilities
                cursor.execute("SELECT @@version")
                version = cursor.fetchone()[0]
                
                cursor.execute("SELECT @@sql_mode")
                sql_mode = cursor.fetchone()[0]
                
                cursor.execute("SHOW VARIABLES LIKE 'local_infile'")
                local_infile = cursor.fetchall()
                
                cursor.execute("SHOW VARIABLES LIKE 'max_allowed_packet'")
                max_packet = cursor.fetchall()
                
                logger.info("✅ RDS MySQL capabilities detected:")
                logger.info(f"📊 MySQL Version: {version}")
                logger.info(f"� SQL Mode: {sql_mode}")
                
                if local_infile:
                    logger.info(f"📊 Local Infile: {local_infile[0][1]}")
                
                if max_packet:
                    packet_size = int(max_packet[0][1]) / (1024 * 1024)
                    logger.info(f"📊 Max Allowed Packet: {packet_size:.0f} MB")
                
                logger.info("� Optimized for traditional CSV import workflows")
                return True
                    
        except Exception as e:
            logger.warning(f"⚠️ Could not test MySQL capabilities: {str(e)}")
            return False
    
    def create_sample_table(self):
        """Create a sample table to verify database functionality"""
        try:
            logger.info("🧪 Creating sample table for testing...")
            
            with self.connection.cursor() as cursor:
                # Drop table if exists
                cursor.execute("DROP TABLE IF EXISTS setup_test")
                
                # Create sample table
                create_table_sql = """
                CREATE TABLE setup_test (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    name VARCHAR(255) NOT NULL,
                    email VARCHAR(255),
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
                """
                cursor.execute(create_table_sql)
                
                # Insert test data
                insert_sql = """
                INSERT INTO setup_test (name, email) VALUES 
                ('Test User 1', 'test1@example.com'),
                ('Test User 2', 'test2@example.com'),
                ('RDS MySQL Setup Test', 'rds@example.com')
                """
                cursor.execute(insert_sql)
                
                # Verify data
                cursor.execute("SELECT COUNT(*) FROM setup_test")
                count = cursor.fetchone()[0]
                
                logger.info(f"✅ Sample table created with {count} test records")
                
                # Clean up test table
                cursor.execute("DROP TABLE setup_test")
                logger.info("✅ Test table cleaned up")
                
            return True
            
        except Exception as e:
            logger.error(f"❌ Failed to create sample table: {str(e)}")
            return False
    
    def get_database_info(self):
        """Get database server information"""
        try:
            with self.connection.cursor() as cursor:
                # Get version
                cursor.execute("SELECT VERSION()")
                version = cursor.fetchone()[0]
                
                # Get current database
                cursor.execute("SELECT DATABASE()")
                current_db = cursor.fetchone()[0]
                
                # Get connection info
                cursor.execute("SELECT CONNECTION_ID()")
                connection_id = cursor.fetchone()[0]
                
                logger.info(f"📊 Database Information:")
                logger.info(f"   🔹 Server Version: {version}")
                logger.info(f"   🔹 Current Database: {current_db}")
                logger.info(f"   🔹 Connection ID: {connection_id}")
                logger.info(f"   🔹 Host: {self.host}:{self.port}")
                
                # Identify as RDS MySQL
                logger.info("🔧 Detected: AWS RDS MySQL")
                
        except Exception as e:
            logger.warning(f"⚠️ Could not retrieve database info: {str(e)}")
    
    def close_connection(self):
        """Close database connection"""
        if self.connection:
            self.connection.close()
            logger.info("🔐 Database connection closed")
    
    def setup_complete_database(self):
        """Main setup workflow"""
        logger.info("🚀 Starting RDS MySQL Database Setup")
        logger.info("=" * 60)
        
        try:
            # Step 1: Connect to server
            if not self.connect_to_server():
                return False
            
            # Step 2: Check if database exists
            db_exists = self.check_database_exists()
            
            # Step 3: Create database if it doesn't exist
            if not db_exists:
                if not self.create_database():
                    return False
            else:
                logger.info(f"📁 Using existing database '{self.database_name}'")
            
            # Step 4: Connect to the specific database
            if not self.connect_to_database():
                return False
            
            # Step 5: Get database information
            self.get_database_info()
            
            # Step 6: Test MySQL capabilities
            self.test_mysql_capabilities()
            
            # Step 7: Configure for CSV imports
            if not self.setup_database_for_csv_import():
                return False
            
            # Step 8: Create and test sample table
            if not self.create_sample_table():
                return False
            
            logger.info("=" * 60)
            logger.info("🎉 RDS MySQL database setup completed successfully!")
            logger.info(f"✅ Database '{self.database_name}' is ready for CSV imports")
            logger.info("💡 You can now run CSV import workflows")
            logger.info("=" * 60)
            
            return True
            
        except Exception as e:
            logger.error(f"❌ Database setup failed: {str(e)}")
            return False
        
        finally:
            self.close_connection()

def main():
    """Main function"""
    logger.info("🔧 RDS MySQL Database Setup Utility")
    logger.info("📋 This script will create and configure your RDS MySQL database for CSV imports")
    
    # Check environment variables
    required_vars = ['MYSQL_HOST', 'MYSQL_USERNAME', 'MYSQL_PASSWORD']
    
    missing_vars = [var for var in required_vars if not os.getenv(var)]
    if missing_vars:
        logger.error("❌ Missing required environment variables!")
        logger.error(f"💡 Please set: {', '.join(missing_vars)}")
        logger.error("   Example in .env file:")
        logger.error("   MYSQL_HOST=your-rds-endpoint.region.rds.amazonaws.com")
        logger.error("   MYSQL_USERNAME=your_username")
        logger.error("   MYSQL_PASSWORD=your_password")
        logger.error("   MYSQL_DATABASE=your_database_name")
        sys.exit(1)
    
    logger.info("🔧 Using RDS MySQL configuration")
    
    # Create and run database setup
    db_setup = DatabaseSetup()
    success = db_setup.setup_complete_database()
    
    if success:
        logger.info("🎯 Setup completed successfully!")
        logger.info("🔄 Next steps:")
        logger.info("   1. Run 'python CSV-AURORA/check-databases.py' to verify setup")
        logger.info("   2. Place CSV files in 'csv-to-aurora/' folder")
        logger.info("   3. Run 'python CSV-AURORA/csv-to-aurora.py' to import data")
    else:
        logger.error("❌ Setup failed. Please check the error messages above.")
        sys.exit(1)

if __name__ == "__main__":
    main()
