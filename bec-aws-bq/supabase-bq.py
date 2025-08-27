from supabase import create_client, Client
from google.cloud import bigquery
import pandas as pd
from datetime import datetime
import os
import json
import uuid
from dotenv import load_dotenv

def main():
    print("Starting supabase-bq.py script...")
    
    # Load environment variables from .env file
    env_path = '/Applications/RF/NTU/SCTP in DSAI/s3-rds-bq-dagster/.env'
    load_dotenv(env_path)
    print(f"Loaded environment from: {env_path}")
    
    # Supabase configuration
    supabase_url = os.getenv('SUPABASE_URL')
    supabase_key = os.getenv('SUPABASE_KEY')
    
    print(f"Supabase URL: {supabase_url[:20] if supabase_url else 'Not set'}...")
    print(f"Supabase Key: {'Set' if supabase_key else 'Not set'}")
    
    # BigQuery configuration
    os.environ['BQ_PROJECT_ID'] = 'dsai-468212'
    os.environ['BQ_DATASET'] = 'olist_data_warehouse'
    bq_project_id = os.getenv('BQ_PROJECT_ID')
    bq_dataset = os.getenv('BQ_DATASET')
    
    print(f"BQ Project ID: {bq_project_id}")
    print(f"BQ Dataset: {bq_dataset}")
    
    if not all([supabase_url, supabase_key, bq_project_id, bq_dataset]):
        print("Missing required environment variables")
        return
    
    try:
        print("Connecting to Supabase...")
        supabase: Client = create_client(supabase_url, supabase_key)
        
        print("Connecting to BigQuery...")
        client = bigquery.Client(project=bq_project_id)
        
        # Get all tables from Supabase
        print("Getting table list from Supabase...")
        
        # List of known tables to process (since we can't easily query information_schema)
        tables_to_process = [
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
        
        print(f"Processing {len(tables_to_process)} known tables")
        
        for table_name in tables_to_process:
                
            print(f"\nProcessing table: {table_name}")
            
            # Check if table has data
            try:
                count_response = supabase.table(table_name).select('*', count='exact').execute()
                row_count = count_response.count if hasattr(count_response, 'count') else 0
                
                print(f"Table {table_name} has {row_count} rows")
                
                if row_count == 0:
                    print(f"Skipping {table_name} - no data")
                    continue
                    
                # Get data from Supabase table
                print(f"Fetching all {row_count} rows from {table_name}...")
                
                # Note: Supabase has a hard limit of 1000 rows per request
                # We'll use 1000 as page size and optimize progress reporting
                page_size = 1000
                print(f"Using Supabase's maximum page size of {page_size} rows per request")
                
                all_data = []
                offset = 0
                batch_number = 0
                estimated_batches = (row_count + page_size - 1) // page_size  # Ceiling division
                
                while True:
                    batch_number += 1
                    end_row = min(offset + page_size, row_count)
                    
                    # Smart progress reporting based on table size
                    if row_count > 50000:  # Large tables: show every 50 batches
                        if batch_number % 50 == 0 or batch_number == 1:
                            progress = len(all_data) / row_count * 100
                            print(f"  Progress: Batch {batch_number}/{estimated_batches} - {len(all_data):,}/{row_count:,} rows ({progress:.1f}%)")
                    elif row_count > 10000:  # Medium tables: show every 10 batches
                        if batch_number % 10 == 0 or batch_number == 1:
                            progress = len(all_data) / row_count * 100
                            print(f"  Progress: Batch {batch_number}/{estimated_batches} - {len(all_data):,}/{row_count:,} rows ({progress:.1f}%)")
                    else:  # Small tables: show every batch
                        print(f"  Fetching rows {offset + 1:,} to {end_row:,}...")
                    
                    data_response = supabase.table(table_name).select('*').range(offset, offset + page_size - 1).execute()
                    
                    if not data_response.data:
                        print(f"  No data returned at offset {offset}")
                        break
                    
                    batch_size = len(data_response.data)
                    all_data.extend(data_response.data)
                    offset += page_size
                    
                    # Break if we've got all the data
                    if batch_size < page_size or offset >= row_count:
                        break
                
                if not all_data:
                    print(f"No data found in {table_name}")
                    continue
                
                print(f"✓ Successfully fetched {len(all_data):,} rows from {table_name} in {batch_number} batches")
                
                # Convert to DataFrame
                df = pd.DataFrame(all_data)
                print(f"DataFrame shape: {df.shape}")
                
                # Create BigQuery table name with prefix
                bq_table_name = f"supabase_{table_name}"
                table_id = f"{bq_project_id}.{bq_dataset}.{bq_table_name}"
                
                print(f"Loading to BigQuery table: {table_id}")
                
                # Configure the load job
                job_config = bigquery.LoadJobConfig(
                    write_disposition="WRITE_TRUNCATE",  # Overwrite table
                    autodetect=True,  # Auto-detect schema
                )
                
                # Load DataFrame to BigQuery
                job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
                job.result()  # Wait for the job to complete
                
                print(f"✓ Successfully loaded {df.shape[0]} rows to {bq_table_name}")
                
                # Update metadata in olist_lmod_tables
                print(f"Updating metadata for {table_name}...")
                current_time = datetime.now().isoformat()
                
                try:
                    # Try to update existing record first
                    update_response = supabase.table('olist_lmod_tables').update({
                        'modified_date': current_time
                    }).eq('table_name', table_name).execute()
                    
                    # If no rows were updated, insert new record
                    if not update_response.data:
                        print(f"No existing record found, inserting new one for {table_name}")
                        insert_response = supabase.table('olist_lmod_tables').insert({
                            'id': str(uuid.uuid4()),
                            'table_name': table_name,
                            'modified_date': current_time
                        }).execute()
                        print(f"✓ Inserted metadata record for {table_name}")
                    else:
                        print(f"✓ Updated metadata record for {table_name}")
                        
                except Exception as meta_error:
                    print(f"Warning: Could not update metadata for {table_name}: {meta_error}")
                
            except Exception as table_error:
                print(f"Error processing table {table_name}: {table_error}")
                continue
        
        print("\nSuabase to BigQuery transfer completed!")
        
    except Exception as e:
        print(f"Error in main process: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()
