from supabase import create_client, Client
from google.cloud import bigquery
import pandas as pd
from datetime import datetime
import os
import json
import uuid
import logging
from dotenv import load_dotenv

# Configure logging
def setup_logging():
    log_filename = f"supabase_bq_transfer_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_filename),
            logging.StreamHandler()  # Also show in console
        ]
    )
    return logging.getLogger(__name__)

def main():
    # Setup logging
    logger = setup_logging()
    
    logger.info("="*60)
    logger.info("STARTING SUPABASE TO BIGQUERY TRANSFER")
    logger.info("="*60)
    
    # Load environment variables from .env file
    env_path = '/Applications/RF/NTU/SCTP in DSAI/s3-rds-bq-dagster/.env'
    load_dotenv(env_path)
    logger.info(f"Loaded environment from: {env_path}")
    
    # Supabase configuration
    supabase_url = os.getenv('SUPABASE_URL')
    supabase_key = os.getenv('SUPABASE_KEY')
    
    logger.info(f"Supabase URL: {supabase_url[:20] if supabase_url else 'Not set'}...")
    logger.info(f"Supabase Key: {'Set' if supabase_key else 'Not set'}")
    
    # BigQuery configuration
    os.environ['BQ_PROJECT_ID'] = 'dsai-468212'
    os.environ['BQ_DATASET'] = 'olist_data_warehouse'
    bq_project_id = os.getenv('BQ_PROJECT_ID')
    bq_dataset = os.getenv('BQ_DATASET')
    
    logger.info(f"BQ Project ID: {bq_project_id}")
    logger.info(f"BQ Dataset: {bq_dataset}")
    
    if not all([supabase_url, supabase_key, bq_project_id, bq_dataset]):
        logger.error("Missing required environment variables")
        return
    
    try:
        logger.info("Connecting to Supabase...")
        supabase: Client = create_client(supabase_url, supabase_key)
        
        logger.info("Connecting to BigQuery...")
        client = bigquery.Client(project=bq_project_id)
        
        # Get all tables from Supabase
        logger.info("Getting table list from Supabase...")
        
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
        
        logger.info(f"Processing {len(tables_to_process)} known tables")
        
        # Track overall progress
        total_tables = len(tables_to_process)
        processed_tables = 0
        total_rows_transferred = 0
        start_time = datetime.now()
        
        for table_name in tables_to_process:
            table_start_time = datetime.now()
            processed_tables += 1
            
            logger.info("="*50)
            logger.info(f"TABLE {processed_tables}/{total_tables}: {table_name}")
            logger.info(f"Started at: {table_start_time.strftime('%Y-%m-%d %H:%M:%S')}")
            logger.info("="*50)
                
            print(f"\nProcessing table: {table_name}")
            
            # Check if table has data
            try:
                count_response = supabase.table(table_name).select('*', count='exact').execute()
                row_count = count_response.count if hasattr(count_response, 'count') else 0
                
                logger.info(f"Table {table_name} has {row_count:,} rows")
                
                if row_count == 0:
                    logger.info(f"Skipping {table_name} - no data")
                    continue
                
                total_rows_transferred += row_count
                    
                # Get data from Supabase table
                logger.info(f"Fetching all {row_count:,} rows from {table_name}...")
                
                # Note: Supabase has a hard limit of 1000 rows per request
                # We'll use 1000 as page size and optimize progress reporting
                page_size = 1000
                logger.info(f"Using Supabase's maximum page size of {page_size} rows per request")
                
                all_data = []
                offset = 0
                batch_number = 0
                estimated_batches = (row_count + page_size - 1) // page_size  # Ceiling division
                
                logger.info(f"Estimated {estimated_batches} batches needed for {table_name}")
                
                while True:
                    batch_number += 1
                    end_row = min(offset + page_size, row_count)
                    
                    # Smart progress reporting based on table size
                    if row_count > 50000:  # Large tables: show every 50 batches
                        if batch_number % 50 == 0 or batch_number == 1:
                            progress = len(all_data) / row_count * 100
                            elapsed = datetime.now() - table_start_time
                            logger.info(f"  Progress: Batch {batch_number}/{estimated_batches} - {len(all_data):,}/{row_count:,} rows ({progress:.1f}%) - Elapsed: {elapsed}")
                    elif row_count > 10000:  # Medium tables: show every 10 batches
                        if batch_number % 10 == 0 or batch_number == 1:
                            progress = len(all_data) / row_count * 100
                            elapsed = datetime.now() - table_start_time
                            logger.info(f"  Progress: Batch {batch_number}/{estimated_batches} - {len(all_data):,}/{row_count:,} rows ({progress:.1f}%) - Elapsed: {elapsed}")
                    else:  # Small tables: show every batch
                        logger.info(f"  Fetching rows {offset + 1:,} to {end_row:,}...")
                    
                    # Use consistent ordering to prevent pagination issues  
                    # Order by the first column (usually primary key) for deterministic results
                    if table_name == 'olist_customers_dataset':
                        order_column = 'customer_id'
                    elif table_name == 'olist_sellers_dataset':
                        order_column = 'seller_id' 
                    elif table_name == 'olist_orders_dataset':
                        order_column = 'order_id'
                    elif table_name == 'olist_products_dataset':
                        order_column = 'product_id'
                    else:
                        order_column = 'created_date'  # fallback for other tables
                    
                    data_response = supabase.table(table_name).select('*').order(order_column, desc=False).range(offset, offset + page_size - 1).execute()
                    
                    if not data_response.data:
                        logger.warning(f"  No data returned at offset {offset}")
                        break
                    
                    batch_size = len(data_response.data)
                    all_data.extend(data_response.data)
                    offset += page_size
                    
                    # Break if we've got all the data
                    if batch_size < page_size or offset >= row_count:
                        break
                
                if not all_data:
                    logger.warning(f"No data found in {table_name}")
                    continue
                
                table_fetch_time = datetime.now() - table_start_time
                logger.info(f"✓ Successfully fetched {len(all_data):,} rows from {table_name} in {batch_number} batches (Time: {table_fetch_time})")
                
                # CRITICAL: Validate row count matches expected
                if len(all_data) != row_count:
                    logger.error(f"❌ ROW COUNT MISMATCH! Expected: {row_count:,}, Got: {len(all_data):,}, Difference: {len(all_data) - row_count}")
                    logger.error(f"This indicates a pagination bug - stopping transfer for {table_name}")
                    continue  # Skip this table
                else:
                    logger.info(f"✅ Row count validation passed: {len(all_data):,} rows")
                
                # Convert to DataFrame
                df = pd.DataFrame(all_data)
                logger.info(f"DataFrame shape: {df.shape}")
                
                # Create BigQuery table name with prefix
                bq_table_name = f"supabase_{table_name}"
                table_id = f"{bq_project_id}.{bq_dataset}.{bq_table_name}"
                
                logger.info(f"Loading to BigQuery table: {table_id}")
                upload_start_time = datetime.now()
                
                # Configure the load job
                job_config = bigquery.LoadJobConfig(
                    write_disposition="WRITE_TRUNCATE",  # Overwrite table
                    autodetect=True,  # Auto-detect schema
                )
                
                # Load DataFrame to BigQuery
                job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
                job.result()  # Wait for the job to complete
                
                upload_time = datetime.now() - upload_start_time
                table_total_time = datetime.now() - table_start_time
                logger.info(f"✓ Successfully loaded {df.shape[0]:,} rows to {bq_table_name} (Upload time: {upload_time}, Total time: {table_total_time})")
                
                # Update metadata in olist_lmod_tables
                logger.info(f"Updating metadata for {table_name}...")
                current_time = datetime.now().isoformat()
                
                try:
                    # Try to update existing record first
                    update_response = supabase.table('olist_lmod_tables').update({
                        'modified_date': current_time
                    }).eq('table_name', table_name).execute()
                    
                    # If no rows were updated, insert new record
                    if not update_response.data:
                        logger.info(f"No existing record found, inserting new one for {table_name}")
                        insert_response = supabase.table('olist_lmod_tables').insert({
                            'id': str(uuid.uuid4()),
                            'table_name': table_name,
                            'modified_date': current_time
                        }).execute()
                        logger.info(f"✓ Inserted metadata record for {table_name}")
                    else:
                        logger.info(f"✓ Updated metadata record for {table_name}")
                        
                except Exception as meta_error:
                    logger.error(f"Warning: Could not update metadata for {table_name}: {meta_error}")
                
            except Exception as table_error:
                logger.error(f"Error processing table {table_name}: {table_error}")
                continue
        
        # Final summary
        total_time = datetime.now() - start_time
        logger.info("="*60)
        logger.info("TRANSFER COMPLETED SUCCESSFULLY!")
        logger.info(f"Total tables processed: {processed_tables}")
        logger.info(f"Total rows transferred: {total_rows_transferred:,}")
        logger.info(f"Total time elapsed: {total_time}")
        logger.info(f"Average speed: {total_rows_transferred/total_time.total_seconds():.0f} rows/second")
        logger.info("="*60)
        
    except Exception as e:
        logger.error(f"Error in main process: {e}")
        import traceback
        logger.error(traceback.format_exc())

if __name__ == "__main__":
    main()
