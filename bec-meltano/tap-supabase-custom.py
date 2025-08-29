#!/usr/bin/env python3
"""
Custom Meltano extractor for Supabase using REST API
This wraps our working Python script as a Singer tap
"""

import sys
import json
import os
import requests
from datetime import datetime
from dotenv import load_dotenv

def get_supabase_data(table_name, supabase_url, supabase_key):
    """Extract data from Supabase table using REST API"""
    headers = {
        'apikey': supabase_key,
        'Authorization': f'Bearer {supabase_key}',
        'Content-Type': 'application/json'
    }
    
    # Get total count first
    count_response = requests.get(
        f'{supabase_url}/rest/v1/{table_name}?select=*&limit=0',
        headers={**headers, 'Prefer': 'count=exact'}
    )
    
    if count_response.status_code != 200:
        print(f"Error getting count for {table_name}: {count_response.text}", file=sys.stderr)
        return []
    
    total_rows = int(count_response.headers.get('Content-Range', '0-0/0').split('/')[-1])
    print(f"Total rows in {table_name}: {total_rows}", file=sys.stderr)
    
    # Get all data with pagination
    all_data = []
    limit = 1000
    
    for offset in range(0, total_rows, limit):
        response = requests.get(
            f'{supabase_url}/rest/v1/{table_name}?select=*&limit={limit}&offset={offset}',
            headers=headers
        )
        
        if response.status_code == 200:
            batch_data = response.json()
            all_data.extend(batch_data)
            print(f"Fetched {len(batch_data)} rows from {table_name} (offset: {offset})", file=sys.stderr)
        else:
            print(f"Error fetching {table_name} at offset {offset}: {response.text}", file=sys.stderr)
            break
    
    return all_data

def output_schema(table_name):
    """Output Singer schema for a table"""
    schema = {
        "type": "SCHEMA",
        "stream": table_name,
        "schema": {
            "type": "object",
            "properties": {}  # Will be dynamically determined from data
        },
        "key_properties": []
    }
    print(json.dumps(schema))

def output_record(table_name, record):
    """Output Singer record"""
    singer_record = {
        "type": "RECORD", 
        "stream": table_name,
        "record": record,
        "time_extracted": datetime.utcnow().isoformat()
    }
    print(json.dumps(singer_record))

def main():
    load_dotenv()
    
    supabase_url = os.getenv('SUPABASE_URL')
    supabase_key = os.getenv('SUPABASE_KEY')
    
    if not supabase_url or not supabase_key:
        print("Error: SUPABASE_URL and SUPABASE_KEY must be set", file=sys.stderr)
        sys.exit(1)
    
    # Tables to extract
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
    
    # Output schemas
    for table in tables:
        output_schema(table)
    
    # Extract and output records
    for table in tables:
        print(f"Extracting {table}...", file=sys.stderr)
        data = get_supabase_data(table, supabase_url, supabase_key)
        
        for record in data:
            output_record(table, record)
        
        print(f"Completed {table}: {len(data)} records", file=sys.stderr)

if __name__ == "__main__":
    main()
