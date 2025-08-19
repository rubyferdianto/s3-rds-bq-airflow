#!/usr/bin/env python3
"""
Check BigQuery Data Import Status
Verifies if data has been successfully imported from RDS MySQL to BigQuery
"""

import os
import json
from google.cloud import bigquery
from dotenv import load_dotenv

# Load environment variables
load_dotenv('../.env')

def setup_bigquery_client():
    """Setup BigQuery client with credentials from environment"""
    credentials_json = os.getenv('GOOGLE_APPLICATION_CREDENTIALS_JSON')
    if not credentials_json:
        print("❌ GOOGLE_APPLICATION_CREDENTIALS_JSON not found in environment")
        return None
    
    try:
        # Parse the JSON credentials
        credentials_dict = json.loads(credentials_json)
        
        # Create BigQuery client
        client = bigquery.Client.from_service_account_info(
            credentials_dict,
            project=credentials_dict['project_id']
        )
        
        print(f"✅ BigQuery client created for project: {credentials_dict['project_id']}")
        return client
    
    except Exception as e:
        print(f"❌ Error setting up BigQuery client: {str(e)}")
        return None

def check_dataset_and_tables(client):
    """Check if dataset exists and list tables"""
    project_id = os.getenv('GCP_PROJECT')
    dataset_id = os.getenv('BQ_DATASET')
    
    if not project_id or not dataset_id:
        print("❌ Missing GCP_PROJECT or BQ_DATASET in environment")
        return
    
    try:
        # Check if dataset exists
        dataset_ref = client.dataset(dataset_id, project=project_id)
        dataset = client.get_dataset(dataset_ref)
        
        print(f"✅ Dataset found: {project_id}.{dataset_id}")
        print(f"   Created: {dataset.created}")
        print(f"   Location: {dataset.location}")
        print(f"   Description: {dataset.description or 'No description'}")
        
        # List tables in the dataset
        tables = list(client.list_tables(dataset))
        
        if tables:
            print(f"\n📊 Tables in dataset ({len(tables)} total):")
            for table in tables:
                # Get table details
                table_ref = client.get_table(table)
                print(f"   - {table.table_id}")
                print(f"     Rows: {table_ref.num_rows:,}")
                print(f"     Size: {table_ref.num_bytes:,} bytes")
                print(f"     Created: {table_ref.created}")
                print(f"     Modified: {table_ref.modified}")
                print()
        else:
            print("\n⚠️ No tables found in the dataset")
            
    except Exception as e:
        print(f"❌ Error checking dataset: {str(e)}")

def sample_data_from_tables(client):
    """Sample data from each table to verify content"""
    project_id = os.getenv('GCP_PROJECT')
    dataset_id = os.getenv('BQ_DATASET')
    
    try:
        dataset_ref = client.dataset(dataset_id, project=project_id)
        tables = list(client.list_tables(dataset_ref))
        
        if not tables:
            print("No tables to sample from")
            return
        
        print("\n🔍 Sample data from tables:")
        print("=" * 60)
        
        for table in tables[:3]:  # Sample first 3 tables
            query = f"""
            SELECT *
            FROM `{project_id}.{dataset_id}.{table.table_id}`
            LIMIT 3
            """
            
            print(f"\nTable: {table.table_id}")
            print("-" * 40)
            
            try:
                results = client.query(query)
                rows = list(results)
                
                if rows:
                    # Print column headers
                    columns = [field.name for field in results.schema]
                    print(f"Columns: {', '.join(columns)}")
                    
                    # Print sample rows
                    for i, row in enumerate(rows, 1):
                        print(f"Row {i}: {dict(row)}")
                else:
                    print("No data in table")
                    
            except Exception as e:
                print(f"Error querying table: {str(e)}")
        
    except Exception as e:
        print(f"❌ Error sampling data: {str(e)}")

def main():
    """Main function"""
    print("🔍 Checking BigQuery Data Import Status")
    print("=" * 50)
    
    # Setup BigQuery client
    client = setup_bigquery_client()
    if not client:
        return
    
    # Check dataset and tables
    check_dataset_and_tables(client)
    
    # Sample data from tables
    sample_data_from_tables(client)
    
    print("\n✅ BigQuery check completed!")

if __name__ == "__main__":
    main()
