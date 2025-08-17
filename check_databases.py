#!/usr/bin/env python3
"""
Simple script to show databases and tables in your RDS instance
"""

import pymysql
import os
from dotenv import load_dotenv
import logging

# Load environment variables
load_dotenv()

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def main():
    # Get configuration
    mysql_host = os.getenv('MYSQL_HOST')
    mysql_port = int(os.getenv('MYSQL_PORT', '3306'))
    mysql_username = os.getenv('MYSQL_USERNAME')
    mysql_password = os.getenv('MYSQL_PASSWORD')
    
    print("🔍 Checking your RDS Instance...")
    print(f"Instance: {mysql_host}")
    print(f"Username: {mysql_username}")
    
    try:
        # Connect to MySQL server (without specifying database)
        connection = pymysql.connect(
            host=mysql_host,
            port=mysql_port,
            user=mysql_username,
            password=mysql_password,
            connect_timeout=30
        )
        
        print("\n✅ Connected to RDS instance successfully!")
        
        # Show all databases
        with connection.cursor() as cursor:
            cursor.execute("SHOW DATABASES")
            databases = cursor.fetchall()
        
        print(f"\n📁 DATABASES in your RDS instance '{mysql_host.split('.')[0]}':")
        print("=" * 50)
        
        user_databases = []
        for db in databases:
            db_name = db[0]
            if db_name not in ['information_schema', 'performance_schema', 'mysql', 'sys']:
                user_databases.append(db_name)
                print(f"  📄 {db_name}")
        
        if not user_databases:
            print("  (No user databases found)")
        
        # For each user database, show tables
        for db_name in user_databases:
            print(f"\n📊 TABLES in database '{db_name}':")
            print("-" * 30)
            
            try:
                # Connect to specific database
                db_connection = pymysql.connect(
                    host=mysql_host,
                    port=mysql_port,
                    user=mysql_username,
                    password=mysql_password,
                    database=db_name
                )
                
                with db_connection.cursor() as cursor:
                    cursor.execute("SHOW TABLES")
                    tables = cursor.fetchall()
                
                if tables:
                    for table in tables:
                        # Get row count for each table
                        with db_connection.cursor() as cursor:
                            cursor.execute(f"SELECT COUNT(*) FROM `{table[0]}`")
                            count = cursor.fetchone()[0]
                        
                        print(f"  📋 {table[0]} ({count:,} rows)")
                else:
                    print("  (No tables found)")
                
                db_connection.close()
                
            except Exception as e:
                print(f"  ❌ Error accessing database '{db_name}': {e}")
        
        connection.close()
        
        print(f"\n{'='*60}")
        print("💡 SUMMARY:")
        print("• Your RDS Instance is working correctly")
        if user_databases:
            print(f"• You have {len(user_databases)} database(s) ready to use")
            print(f"• Current database in .env: {os.getenv('MYSQL_DATABASE')}")
        else:
            print("• No databases created yet - you can create one now")
        print("='*60")
        
    except Exception as e:
        print(f"\n❌ Error connecting to RDS: {e}")

if __name__ == "__main__":
    main()
