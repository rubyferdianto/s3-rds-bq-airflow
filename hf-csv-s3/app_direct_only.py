import gradio as gr
import boto3
import os
import pandas as pd
from dotenv import load_dotenv
from supabase import create_client, Client
import io
import requests
from sqlalchemy import create_engine, text
import sqlalchemy as sa
from urllib.parse import quote_plus

# Load environment variables from .env file (for local testing)
load_dotenv()

# Configure S3 client (use env vars or Hugging Face secrets)
try:
    s3 = boto3.client(
        "s3",
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
        region_name=os.getenv("AWS_REGION", "ap-southeast-1")
    )
    s3_available = True
except Exception as e:
    s3_available = False
    print(f"Warning: S3 client setup failed: {e}")

# Configure Supabase client
try:
    # Use environment variables instead of hardcoded credentials
    supabase_url = os.getenv("SUPABASE_URL", "https://tpldiheffudabhiyowod.supabase.co")
    supabase_key = os.getenv("SUPABASE_KEY")  # Remove hardcoded key
    
    if supabase_url and supabase_key:
        supabase: Client = create_client(supabase_url, supabase_key)
        supabase_available = True
        print("✅ Supabase client configured successfully")
    else:
        supabase_available = False
        print("Warning: Supabase credentials not found in environment variables")
except Exception as e:
    supabase_available = False
    print(f"Warning: Supabase client setup failed: {e}")

# Configure SQLAlchemy engine for direct PostgreSQL connection to Supabase
try:
    # Extract database connection info from Supabase URL
    # Format: postgresql://[user]:[password]@[host]:[port]/[database]
    
    # For Supabase, we can use the REST API to get connection details
    # Or construct it manually - Supabase uses PostgreSQL on port 5432
    db_host = "db.tpldiheffudabhiyowod.supabase.co"
    db_port = "5432"
    db_name = "postgres"
    db_user = "becuser"

    # For the password, we need the database password (different from API key)
    # This should be set in environment variables for security
    db_password = os.getenv("DB_PASSWORD", os.getenv("SUPABASE_DB_PASSWORD", "your_database_password_here"))
    
    # Create PostgreSQL connection string
    if db_password != "your_database_password_here":
        connection_string = f"postgresql://{db_user}:{quote_plus(db_password)}@{db_host}:{db_port}/{db_name}"
        engine = create_engine(
            connection_string,
            pool_size=10,
            max_overflow=20,
            pool_pre_ping=True,
            echo=False  # Set to True for SQL logging
        )
        # Temporarily disable direct PostgreSQL due to DNS resolution issues
        db_available = False  # Force use of REST API
        print("⚠️ Direct PostgreSQL disabled due to network issues. Using REST API method.")
    else:
        engine = None
        db_available = False
        print("⚠️ Database password not configured. Using API method as fallback.")
        
except Exception as e:
    engine = None
    db_available = False
    print(f"⚠️ PostgreSQL engine setup failed: {e}")
    print("Using Supabase API method as fallback.")

BUCKET_NAME = "bec-bucket-aws"
FOLDER = "s3-to-rds"

def upload_csv_to_s3(file):
    if not s3_available:
        return "❌ Error: AWS credentials not configured. Please set AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY"
    
    try:
        df = pd.read_csv(file.name)  # Validate CSV
        filename = os.path.basename(file.name)

        s3.upload_file(file.name, BUCKET_NAME, f"{FOLDER}/{filename}")
        return f"✅ Uploaded {filename} to s3://{BUCKET_NAME}/{FOLDER}/{filename}"
    except Exception as e:
        return f"❌ Error: {str(e)}"

def upload_csv_direct_to_supabase(file):
    """
    Streamlined direct CSV processing workflow:
    CSV File → Clean Data → Truncate Table → Insert → Done
    No storage uploads, no archival - just pure performance!
    """
    if not supabase_available:
        return "❌ Error: Supabase credentials not configured"
    
    workflow_log = []
    
    try:
        # Load and validate CSV
        df = pd.read_csv(file.name)
        filename = os.path.basename(file.name)
        timestamp = pd.Timestamp.now().strftime("%Y%m%d_%H%M%S")
        table_name = filename.replace('.csv', '').replace('-', '_').replace(' ', '_').lower()
        
        # Check file size for information
        file_size_mb = os.path.getsize(file.name) / (1024 * 1024)
        
        workflow_log.append(f"🚀 DIRECT PROCESSING MODE - {filename}")
        workflow_log.append(f"📊 CSV Data: {len(df):,} rows, {len(df.columns)} columns")
        workflow_log.append(f"📏 File size: {file_size_mb:.2f} MB")
        workflow_log.append(f"🎯 Target table: {table_name}")
        workflow_log.append("⚡ Direct Mode: CSV → Clean → Truncate → Insert → Done")
        
        # ==========================================
        # STEP 1: Process CSV data for database import
        # ==========================================
        workflow_log.append(f"\n💾 STEP 1: Processing CSV data...")
        
        try:
            # Clean column names for database compatibility
            cleaned_columns = {}
            for col in df.columns:
                clean_col = col.replace(' ', '_').replace('-', '_').replace('.', '_').replace('(', '').replace(')', '').lower()
                cleaned_columns[col] = clean_col
            
            df_clean = df.rename(columns=cleaned_columns)
            
            # Add CREATED_DATE column with current system date
            df_clean['created_date'] = pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')
            workflow_log.append(f"✅ Added CREATED_DATE column: {df_clean['created_date'].iloc[0]}")
            workflow_log.append(f"✅ Cleaned {len(df_clean.columns)} column names for database compatibility")
            
            inserted_count = 0
            database_success = False
            
        except Exception as processing_error:
            workflow_log.append(f"❌ Failed to process CSV data: {str(processing_error)}")
            database_success = False
            inserted_count = 0
            df_clean = None
        
        # ==========================================
        # STEP 2: Database import with smart truncation
        # ==========================================
        if df_clean is not None:
            try:
                if db_available and engine:
                    # Method 1: Direct PostgreSQL connection
                    workflow_log.append("\n🔄 STEP 2: Using direct PostgreSQL connection...")
                    
                    # Use replace mode - fastest for direct processing
                    df_clean.to_sql(
                        name=table_name,
                        con=engine,
                        schema='public',
                        if_exists='replace',
                        index=False,
                        method='multi',
                        chunksize=10000
                    )
                    inserted_count = len(df_clean)
                    method_used = "PostgreSQL (pandas.to_sql - replace)"
                    database_success = True
                    
                else:
                    # Method 2: Supabase REST API with smart truncation
                    workflow_log.append("\n🔄 STEP 2: Using Supabase REST API...")
                    
                    # Check if table exists
                    table_exists = False
                    try:
                        test_query = supabase.table(table_name).select("*").limit(1).execute()
                        workflow_log.append(f"✅ Table '{table_name}' exists")
                        table_exists = True
                    except Exception as table_error:
                        workflow_log.append(f"📝 Table '{table_name}' doesn't exist - will be created on first insert")
                    
                    # Prepare records with smart data type conversion
                    records = df_clean.to_dict('records')
                    workflow_log.append(f"🔄 Preparing {len(records):,} records for database insertion...")
                    
                    for record in records:
                        for key, value in record.items():
                            if pd.isna(value):
                                record[key] = None
                            elif isinstance(value, float):
                                # Convert float to int if it's a whole number (e.g., 287.0 → 287)
                                if value.is_integer():
                                    record[key] = int(value)
                                else:
                                    record[key] = value
                            # Handle numpy int64/float64 types
                            elif hasattr(value, 'dtype'):
                                if 'int' in str(value.dtype):
                                    record[key] = int(value)
                                elif 'float' in str(value.dtype):
                                    record[key] = float(value)
                    
                    workflow_log.append("✅ Data type conversion completed")
                    
                    # Smart truncation using your custom function
                    if table_exists:
                        truncation_successful = False
                        try:
                            workflow_log.append(f"🗑️ Truncating table '{table_name}' using custom PostgreSQL function...")
                            
                            # Use your custom olist_truncate_table function (BEST!)
                            try:
                                truncate_result = supabase.rpc('olist_truncate_table', {'table_name': table_name}).execute()
                                workflow_log.append(f"✅ Table '{table_name}' truncated successfully using olist_truncate_table()")
                                workflow_log.append("🚀 TRUNCATE TABLE with RESTART IDENTITY CASCADE executed!")
                                truncation_successful = True
                            except Exception as rpc_error:
                                error_msg = str(rpc_error)
                                workflow_log.append(f"⚠️ Custom truncate function failed: {error_msg}")
                                
                                # Fallback: REST API universal delete
                                workflow_log.append("🔄 Falling back to REST API delete method...")
                                try:
                                    delete_result = supabase.table(table_name).delete().neq('created_date', '1900-01-01').execute()
                                    workflow_log.append(f"✅ Table '{table_name}' truncated successfully using REST API")
                                    truncation_successful = True
                                except Exception as delete_error:
                                    workflow_log.append(f"⚠️ REST API delete failed: {str(delete_error)}")
                        except Exception as general_error:
                            workflow_log.append(f"⚠️ Truncation error: {str(general_error)}")
                        
                        if not truncation_successful:
                            workflow_log.append("🔄 Proceeding with insert (may create duplicates if re-uploading)...")
                        else:
                            workflow_log.append("🎯 Table successfully cleared - ready for fresh data!")
                    
                    # High-performance batch insertion
                    batch_size = 10000
                    total_batches = (len(records) + batch_size - 1) // batch_size
                    successful_inserts = 0
                    
                    workflow_log.append(f"📥 Inserting data in {total_batches} batches of {batch_size:,} records each...")
                    
                    for i in range(0, len(records), batch_size):
                        batch = records[i:i + batch_size]
                        batch_num = i // batch_size + 1
                        
                        try:
                            if table_exists and truncation_successful:
                                # Table was cleared, use fastest insert
                                result = supabase.table(table_name).insert(batch).execute()
                                successful_inserts += len(batch)
                                workflow_log.append(f"  ✅ Batch {batch_num}/{total_batches} completed ({len(batch):,} records)")
                            else:
                                # Table doesn't exist or truncation failed, use upsert
                                result = supabase.table(table_name).upsert(batch).execute()
                                successful_inserts += len(batch)
                                workflow_log.append(f"  ✅ Batch {batch_num}/{total_batches} completed ({len(batch):,} records - upsert)")
                        except Exception as batch_error:
                            # Final fallback: try regular insert (may create table)
                            try:
                                result = supabase.table(table_name).insert(batch).execute()
                                successful_inserts += len(batch)
                                workflow_log.append(f"  ✅ Batch {batch_num}/{total_batches} completed ({len(batch):,} records - table created)")
                            except Exception as insert_error:
                                workflow_log.append(f"  ⚠️ Batch {batch_num}/{total_batches} failed: {str(insert_error)}")
                                if "PGRST205" in str(insert_error):
                                    workflow_log.append(f"  💡 Table '{table_name}' doesn't exist - create it manually in Supabase")
                    
                    inserted_count = successful_inserts
                    method_used = f"Supabase REST API - Direct Processing ({'success' if successful_inserts > 0 else 'failed'})"
                    database_success = successful_inserts > 0
                
                workflow_log.append(f"\n✅ STEP 2 Complete: {inserted_count:,} rows imported to table '{table_name}'")
                workflow_log.append(f"📋 Method: {method_used}")
                
            except Exception as db_error:
                workflow_log.append(f"❌ STEP 2 Failed: Database import error - {str(db_error)}")
                database_success = False
        else:
            workflow_log.append("❌ STEP 2 Skipped: Could not process CSV data")
            database_success = False
        
        # ==========================================
        # Generate final report
        # ==========================================
        success_rate = (inserted_count / len(df)) * 100 if len(df) > 0 else 0
        processing_time = "Direct processing - no storage overhead"
        
        final_report = f"""
🚀 DIRECT PROCESSING COMPLETED

📁 File Processing:
   • Original: {filename}
   • Target Table: {table_name}
   • File Size: {file_size_mb:.2f} MB
   • Rows: {len(df):,}
   • Columns: {len(df.columns)}

📊 Database Import:
   • Success Rate: {success_rate:.1f}% ({inserted_count:,}/{len(df):,} rows)
   • Method: {method_used if database_success else 'Failed'}
   • Processing: {processing_time}

⚡ Performance Benefits:
   • No storage upload delays
   • No file size limitations
   • No storage costs
   • Fastest possible processing

{'🎉 DIRECT PROCESSING SUCCESSFUL!' if database_success else '⚠️ PROCESSING INCOMPLETE - See details above'}

📝 Detailed Log:
{chr(10).join(workflow_log)}
"""
        
        return final_report
        
    except Exception as e:
        workflow_log.append(f"❌ CRITICAL ERROR: {str(e)}")
        return f"❌ Direct processing failed: {str(e)}\n\n📝 Log:\n" + "\n".join(workflow_log)

def handle_upload(file, destination):
    if file is None:
        return "❌ Error: Please select a file to upload"
    
    if destination == "AWS S3":
        return upload_csv_to_s3(file)
    elif destination == "Supabase":
        return upload_csv_direct_to_supabase(file)
    else:
        return "❌ Error: Please select a destination"

with gr.Blocks() as demo:
    gr.Markdown("### Upload CSV to Cloud Storage")
    gr.Markdown("**🚀 Direct Processing Mode**: Fastest CSV to database import - no storage overhead!")
    
    with gr.Group():
        gr.Markdown("#### Import Selection")
        destination_radio = gr.Radio(
            choices=["AWS S3", "Supabase"],
            value="Supabase",
            label="Select Destination",
            info="Supabase uses direct processing - fastest for any file size!"
        )
    
    file_input = gr.File(file_types=[".csv"], label="Upload CSV")
    upload_btn = gr.Button("Upload", variant="primary")
    output = gr.Textbox(label="Result")

    upload_btn.click(handle_upload, inputs=[file_input, destination_radio], outputs=output)

if __name__ == "__main__":
    demo.launch()
