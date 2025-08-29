#!/usr/bin/env python3
"""
Quick BigQuery verification script to check if data was actually transferred
"""

import os
import json
from google.cloud import bigquery
from google.oauth2 import service_account
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

def verify_bigquery_data():
    """Verify that data was actually transferred to BigQuery"""
    
    # Setup BigQuery client
    credentials_json = os.getenv('GOOGLE_APPLICATION_CREDENTIALS_JSON')
    if not credentials_json:
        print("❌ GOOGLE_APPLICATION_CREDENTIALS_JSON not set")
        return False
    
    credentials_dict = json.loads(credentials_json)
    credentials = service_account.Credentials.from_service_account_info(credentials_dict)
    project_id = credentials_dict.get('project_id')
    
    client = bigquery.Client(project=project_id, credentials=credentials)
    dataset_id = f"{project_id}.olist_data_warehouse"
    
    print(f"🔍 Checking BigQuery dataset: {dataset_id}")
    
    # Get all tables in the dataset
    try:
        dataset = client.get_dataset(dataset_id)
        tables = list(client.list_tables(dataset))
        
        if not tables:
            print("❌ No tables found in BigQuery dataset")
            return False
        
        print(f"✅ Found {len(tables)} tables in BigQuery")
        
        # Check row counts for each table
        total_rows = 0
        for table in tables:
            table_ref = client.get_table(table.reference)
            row_count = table_ref.num_rows
            
            if row_count > 0:
                # Sample a few rows to verify actual data
                query = f"""
                SELECT COUNT(*) as total_rows
                FROM `{dataset_id}.{table.table_id}`
                """
                result = client.query(query).result()
                actual_count = next(result)[0]
                
                print(f"✅ {table.table_id}: {actual_count:,} rows")
                total_rows += actual_count
                
                # Show sample data from the first non-empty table
                if actual_count > 0:
                    sample_query = f"""
                    SELECT *
                    FROM `{dataset_id}.{table.table_id}`
                    LIMIT 3
                    """
                    sample_result = client.query(sample_query).result()
                    print(f"   📋 Sample data from {table.table_id}:")
                    for row in sample_result:
                        sample_data = dict(row)
                        # Show only first few columns to avoid long output
                        sample_short = {k: v for i, (k, v) in enumerate(sample_data.items()) if i < 3}
                        print(f"      {sample_short}")
                    print()
                    break  # Only show sample from first table with data
            else:
                print(f"📋 {table.table_id}: 0 rows (empty)")
        
        print(f"📊 Total rows across all tables: {total_rows:,}")
        
        if total_rows > 0:
            print("🎉 SUCCESS: Data was successfully transferred to BigQuery!")
            return True
        else:
            print("⚠️ WARNING: Tables exist but contain no data")
            return False
            
    except Exception as e:
        print(f"❌ Error checking BigQuery: {e}")
        return False

if __name__ == "__main__":
    verify_bigquery_data()
