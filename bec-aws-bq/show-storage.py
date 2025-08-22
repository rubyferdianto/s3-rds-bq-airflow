#!/usr/bin/env python3
"""
Simple script to show exactly what's in your RDS storage
"""

import pymysql
import os
from dotenv import load_dotenv

load_dotenv()

def show_database_contents():
    """Show what's actually stored in your RDS"""
    
    connection = pymysql.connect(
        host=os.getenv('MYSQL_HOST'),
        port=int(os.getenv('MYSQL_PORT', '3306')),
        user=os.getenv('MYSQL_USERNAME'),
        password=os.getenv('MYSQL_PASSWORD'),
        database=os.getenv('MYSQL_DATABASE')
    )
    
    print("🗄️  WHAT'S STORED IN YOUR RDS INSTANCE:")
    print("=" * 60)
    
    with connection.cursor() as cursor:
        # Show tables
        cursor.execute("SHOW TABLES")
        tables = cursor.fetchall()
        
        print(f"📁 Database: {os.getenv('MYSQL_DATABASE')}")
        print(f"🌐 Physical Location: AWS {os.getenv('AWS_DEFAULT_REGION')} data centers")
        print(f"💾 Storage Type: EBS (Elastic Block Store)")
        print("\n📊 TABLES AND DATA:")
        
        total_rows = 0
        for table in tables:
            table_name = table[0]
            
            # Get row count
            cursor.execute(f"SELECT COUNT(*) FROM `{table_name}`")
            row_count = cursor.fetchone()[0]
            total_rows += row_count
            
            # Get table size (approximate)
            cursor.execute(f"""
                SELECT 
                    ROUND(((data_length + index_length) / 1024 / 1024), 2) AS 'Size (MB)'
                FROM information_schema.tables 
                WHERE table_schema = %s AND table_name = %s
            """, (os.getenv('MYSQL_DATABASE'), table_name))
            
            size_result = cursor.fetchone()
            size_mb = size_result[0] if size_result and size_result[0] else 0
            
            print(f"  📋 {table_name}")
            print(f"     └── {row_count:,} rows, ~{size_mb} MB")
        
        print(f"\n📈 TOTAL: {total_rows:,} rows across {len(tables)} tables")
        
        # Show database size
        cursor.execute(f"""
            SELECT 
                ROUND(SUM(data_length + index_length) / 1024 / 1024, 2) AS 'Total DB Size (MB)'
            FROM information_schema.tables 
            WHERE table_schema = %s
        """, (os.getenv('MYSQL_DATABASE'),))
        
        total_size = cursor.fetchone()[0] or 0
        print(f"💾 TOTAL DATABASE SIZE: ~{total_size} MB")
    
    connection.close()

if __name__ == "__main__":
    show_database_contents()
