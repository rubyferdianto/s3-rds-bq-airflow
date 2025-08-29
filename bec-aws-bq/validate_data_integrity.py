#!/usr/bin/env python3
"""
Data Integrity Validator for Supabase to BigQuery Transfer
This script validates that data was transferred correctly by comparing row counts.
"""

from supabase import create_client
from google.cloud import bigquery
from dotenv import load_dotenv
import os

def main():
    print("=" * 60)
    print("DATA INTEGRITY VALIDATION")
    print("=" * 60)
    
    # Load environment
    load_dotenv('/Applications/RF/NTU/SCTP in DSAI/s3-rds-bq-dagster/.env')
    
    # Setup connections
    supabase_url = os.getenv('SUPABASE_URL')
    supabase_key = os.getenv('SUPABASE_KEY')
    supabase = create_client(supabase_url, supabase_key)
    
    os.environ['BQ_PROJECT_ID'] = 'dsai-468212'
    os.environ['BQ_DATASET'] = 'olist_data_warehouse'
    client = bigquery.Client(project='dsai-468212')
    
    # Tables to validate
    tables = [
        'olist_customers_dataset',
        'olist_geolocation_dataset', 
        'olist_order_items_dataset',
        'olist_order_payments_dataset',
        'olist_order_reviews_dataset',
        'olist_orders_dataset',
        'olist_products_dataset',
        'olist_sellers_dataset',
        'product_category_name_translation'
    ]
    
    print(f"Validating {len(tables)} tables...\n")
    
    total_issues = 0
    
    for table_name in tables:
        print(f"Checking {table_name}...")
        
        try:
            # Get Supabase count
            sb_response = supabase.table(table_name).select('*', count='exact').execute()
            sb_count = sb_response.count if hasattr(sb_response, 'count') else 0
            
            # Get BigQuery count
            bq_table_name = f"supabase_{table_name}"
            bq_query = f"SELECT COUNT(*) as total FROM dsai-468212.olist_data_warehouse.{bq_table_name}"
            bq_result = client.query(bq_query).result()
            bq_count = list(bq_result)[0].total
            
            # Compare
            difference = sb_count - bq_count
            
            print(f"  Supabase: {sb_count:,} rows")
            print(f"  BigQuery: {bq_count:,} rows")
            
            if difference == 0:
                print(f"  ✅ MATCH")
            else:
                print(f"  ❌ MISMATCH - Difference: {difference:+,} rows")
                total_issues += 1
                
        except Exception as e:
            print(f"  ❌ ERROR: {e}")
            total_issues += 1
            
        print()
    
    print("=" * 60)
    if total_issues == 0:
        print("🎉 ALL TABLES VALIDATED SUCCESSFULLY!")
        print("Data integrity is confirmed.")
    else:
        print(f"⚠️  FOUND {total_issues} ISSUES")
        print("Data transfer needs to be re-run with fixes.")
    print("=" * 60)

if __name__ == "__main__":
    main()
