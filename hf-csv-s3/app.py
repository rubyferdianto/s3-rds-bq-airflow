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
SUPABASE_BUCKET = "olist-bucket"  # Supabase storage bucket name
SUPABASE_FOLDER = "csv-import"    # Supabase folder for CSV imports
SUPABASE_ARCHIVE_FOLDER = "csv-imported"  # Archive folder for successfully processed files

def create_table_from_dataframe(df_clean, table_name):
    """Create a table dynamically based on DataFrame structure"""
    if not supabase_available:
        return False, "Supabase not available"
    
    try:
        # Generate SQL CREATE TABLE statement from DataFrame
        sql_columns = []
        
        for column, dtype in df_clean.dtypes.items():
            if column == 'created_date':
                sql_columns.append(f'"{column}" TIMESTAMP')
            elif 'int' in str(dtype):
                sql_columns.append(f'"{column}" INTEGER')
            elif 'float' in str(dtype):
                sql_columns.append(f'"{column}" NUMERIC')
            elif 'bool' in str(dtype):
                sql_columns.append(f'"{column}" BOOLEAN')
            else:
                sql_columns.append(f'"{column}" TEXT')
        
        # Create SQL statement
        create_sql = f'''
        CREATE TABLE IF NOT EXISTS "public"."{table_name}" (
            "id" SERIAL PRIMARY KEY,
            {", ".join(sql_columns)}
        );
        '''
        
        # Try exec_sql RPC first (if available)
        try:
            result = supabase.rpc('exec_sql', {'sql': create_sql}).execute()
            if result.data and 'successfully' in str(result.data):
                return True, f"Table '{table_name}' created via SQL"
        except:
            pass  # RPC not available, try alternative method
        
        # Alternative: Create table by inserting sample data
        sample_record = {}
        for column in df_clean.columns:
            if column == 'created_date':
                sample_record[column] = pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')
            elif 'int' in str(df_clean[column].dtype):
                sample_record[column] = 0
            elif 'float' in str(df_clean[column].dtype):
                sample_record[column] = 0.0
            elif 'bool' in str(df_clean[column].dtype):
                sample_record[column] = False
            else:
                sample_record[column] = 'TEMP_RECORD_FOR_SCHEMA'
        
        # Insert sample record to create table structure
        result = supabase.table(table_name).insert([sample_record]).execute()
        
        if result.data and len(result.data) > 0:
            # Delete the temporary record
            try:
                supabase.table(table_name).delete().eq('id', result.data[0]['id']).execute()
            except:
                pass  # If delete fails, the temp record will remain but table is created
            
            return True, f"Table '{table_name}' created via sample insert"
        else:
            return False, "Failed to create table"
            
    except Exception as create_error:
        return False, f"Table creation error: {str(create_error)}"

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

def upload_csv_with_complete_workflow(file):
    """
    Complete 4-step CSV workflow:
    1. Upload CSV to csv-import folder
    2. Import data to Supabase database
    3. If successful, move file to csv-imported folder (archive)
    4. Delete file from csv-import folder (cleanup)
    """
    if not supabase_available:
        return "❌ Error: Supabase credentials not configured"
    
    workflow_log = []
    
    try:
        # Load and validate CSV
        df = pd.read_csv(file.name)
        filename = os.path.basename(file.name)
        timestamp = pd.Timestamp.now().strftime("%Y%m%d_%H%M%S")
        unique_filename = f"{timestamp}_{filename}"
        table_name = filename.replace('.csv', '').replace('-', '_').replace(' ', '_').lower()
        
        workflow_log.append(f"🚀 Starting 4-Step Workflow for {filename}")
        workflow_log.append(f"📊 Loaded CSV: {len(df):,} rows, {len(df.columns)} columns")
        
        # ==========================================
        # STEP 1: Upload CSV to csv-import folder
        # ==========================================
        workflow_log.append("\n📥 STEP 1: Uploading to csv-import folder...")
        
        # Ensure bucket exists
        try:
            buckets = supabase.storage.list_buckets()
            bucket_names = [bucket.name for bucket in buckets]
            
            if SUPABASE_BUCKET not in bucket_names:
                supabase.storage.create_bucket(SUPABASE_BUCKET)
                workflow_log.append(f"✅ Created bucket: {SUPABASE_BUCKET}")
            else:
                workflow_log.append(f"✅ Bucket exists: {SUPABASE_BUCKET}")
                
        except Exception as bucket_error:
            workflow_log.append(f"⚠️ Bucket warning: {bucket_error}")
        
        # Upload to csv-import folder
        import_file_path = f"{SUPABASE_FOLDER}/{unique_filename}"
        
        with open(file.name, 'rb') as f:
            file_content = f.read()
        
        upload_result = supabase.storage.from_(SUPABASE_BUCKET).upload(
            path=import_file_path,
            file=file_content,
            file_options={
                "content-type": "text/csv",
                "upsert": "true"
            }
        )
        
        workflow_log.append(f"✅ Step 1 Complete: Uploaded to {import_file_path}")
        
        # ==========================================
        # STEP 2: Import CSV data from Supabase bucket to database
        # ==========================================
        workflow_log.append("\n💾 STEP 2: Reading CSV from bucket and importing to database...")
        
        try:
            # Download CSV from Supabase bucket
            workflow_log.append(f"📥 Reading CSV from bucket: {import_file_path}")
            csv_response = supabase.storage.from_(SUPABASE_BUCKET).download(import_file_path)
            
            # Convert bytes to pandas DataFrame
            csv_content = io.StringIO(csv_response.decode('utf-8'))
            df_from_bucket = pd.read_csv(csv_content)
            
            workflow_log.append(f"✅ Successfully read CSV from bucket: {len(df_from_bucket):,} rows, {len(df_from_bucket.columns)} columns")
            
            # Clean column names for database compatibility
            cleaned_columns = {}
            for col in df_from_bucket.columns:
                clean_col = col.replace(' ', '_').replace('-', '_').replace('.', '_').replace('(', '').replace(')', '').lower()
                cleaned_columns[col] = clean_col
            
            df_clean = df_from_bucket.rename(columns=cleaned_columns)
            
            # Add CREATED_DATE column with current system date
            df_clean['created_date'] = pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')
            workflow_log.append(f"✅ Added CREATED_DATE column: {df_clean['created_date'].iloc[0]}")
            
            inserted_count = 0
            database_success = False
            
        except Exception as download_error:
            workflow_log.append(f"❌ Failed to read CSV from bucket: {str(download_error)}")
            database_success = False
            inserted_count = 0
            df_clean = None
        
        # Only proceed with database import if CSV was successfully read from bucket
        if df_clean is not None:
            try:
                if db_available and engine:
                    # Method 1: Direct PostgreSQL connection
                    workflow_log.append("🔄 Using direct PostgreSQL connection...")
                    
                    # Use append mode to add new data
                    df_clean.to_sql(
                        name=table_name,
                        con=engine,
                        schema='public',
                        if_exists='replace',  # Append to existing table
                        index=False,
                        method='multi',
                        chunksize=1000
                    )
                    inserted_count = len(df_clean)
                    method_used = "PostgreSQL (pandas.to_sql - replace)"
                    database_success = True
                    
                else:
                    # Method 2: Supabase REST API with dynamic table creation
                    workflow_log.append("🔄 Using Supabase REST API...")
                    
                    # Check if table exists, create if not
                    table_exists = False
                    try:
                        # Try to query the table to see if it exists
                        test_query = supabase.table(table_name).select("*").limit(1).execute()
                        workflow_log.append(f"✅ Table '{table_name}' exists")
                        table_exists = True
                    except Exception as table_error:
                        # Table doesn't exist, create it dynamically
                        workflow_log.append(f"📝 Table '{table_name}' doesn't exist")
                        workflow_log.append("🔄 For Hugging Face deployment: Tables must be created manually in Supabase")
                        workflow_log.append("💡 Or enable RLS and create exec_sql function in your Supabase project")
                        
                        # Try direct insert anyway - sometimes works for simple cases
                        workflow_log.append("🔄 Attempting direct insert (may create table automatically)...")
                    
                    records = df_clean.to_dict('records')
                    # Handle NaN values and ensure CREATED_DATE is properly formatted
                    for record in records:
                        for key, value in record.items():
                            if pd.isna(value):
                                record[key] = None
                        # Ensure created_date is included
                        if 'created_date' not in record:
                            record['created_date'] = pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')
                    
                    # Truncate table before inserting new data (replace behavior)
                    if table_exists:
                        try:
                            workflow_log.append(f"🗑️ Truncating table '{table_name}' using universal filter...")
                            
                            # Your brilliant idea: Use "WHERE 1=1" equivalent with Supabase REST API
                            # We'll use a condition that should always be true for all records
                            
                            # Strategy 1: Try neq with created_date (we know this column exists)
                            try:
                                delete_result = supabase.table(table_name).delete().neq('created_date', '1900-01-01').execute()
                                workflow_log.append(f"✅ Table '{table_name}' truncated successfully using created_date filter")
                            except Exception as neq_error:
                                workflow_log.append(f"⚠️ Created_date filter (neq) failed: {str(neq_error)}")
                                
                                # Strategy 2: Try using any other column with impossible value
                                try:
                                    # Get table structure first
                                    sample = supabase.table(table_name).select("*").limit(1).execute()
                                    if sample.data and len(sample.data) > 0:
                                        first_column = list(sample.data[0].keys())[0]
                                        workflow_log.append(f"🔄 Trying universal delete with '{first_column}' column...")
                                        delete_result = supabase.table(table_name).delete().neq(first_column, '___IMPOSSIBLE_VALUE___').execute()
                                        workflow_log.append(f"✅ Table '{table_name}' truncated successfully using '{first_column}' column")
                                    else:
                                        raise Exception("Table appears empty or inaccessible")
                                except Exception as column_error:
                                    workflow_log.append(f"⚠️ Column-based delete also failed: {str(column_error)}")
                                    raise truncate_error  # Re-raise to trigger fallback
                                        
                        except Exception as truncate_error:
                            workflow_log.append(f"⚠️ All truncation attempts failed: {str(truncate_error)}")
                            workflow_log.append("🔄 Proceeding with upsert instead (may create duplicates)...")
                    
                    # Insert in batches with upsert to handle duplicates
                    batch_size = 1000
                    total_batches = (len(records) + batch_size - 1) // batch_size
                    successful_inserts = 0
                    
                    for i in range(0, len(records), batch_size):
                        batch = records[i:i + batch_size]
                        batch_num = i // batch_size + 1
                        
                        try:
                            # Use upsert to handle duplicates gracefully
                            result = supabase.table(table_name).upsert(batch).execute()
                            successful_inserts += len(batch)
                            workflow_log.append(f"  ✅ Batch {batch_num}/{total_batches} completed (upsert)")
                        except Exception as batch_error:
                            # If upsert fails, try regular insert (creates table if not exists)
                            try:
                                result = supabase.table(table_name).insert(batch).execute()
                                successful_inserts += len(batch)
                                workflow_log.append(f"  ✅ Batch {batch_num}/{total_batches} completed (insert - table created)")
                            except Exception as insert_error:
                                workflow_log.append(f"  ⚠️ Batch {batch_num}/{total_batches} failed: {str(insert_error)}")
                                # Provide helpful guidance for Hugging Face deployment
                                if "PGRST205" in str(insert_error) or "schema cache" in str(insert_error):
                                    workflow_log.append(f"  💡 Table '{table_name}' doesn't exist in Supabase")
                                    workflow_log.append(f"  📝 For Hugging Face deployment, create this table manually:")
                                    workflow_log.append(f"     CREATE TABLE {table_name} (id SERIAL PRIMARY KEY, ...);")
                                elif "permission" in str(insert_error).lower():
                                    workflow_log.append(f"  💡 Permission issue - check RLS policies in Supabase")
                    
                    inserted_count = successful_inserts
                    method_used = f"Supabase REST API - {'Truncate & Insert' if table_exists and successful_inserts > 0 else 'Insert Only'} ({'success' if successful_inserts > 0 else 'failed'})"
                    database_success = successful_inserts > 0
                
                workflow_log.append(f"✅ Step 2 Complete: {inserted_count:,} rows imported to table '{table_name}'")
                workflow_log.append(f"📋 Method: {method_used}")
                
            except Exception as db_error:
                workflow_log.append(f"❌ Step 2 Failed: Database import error - {str(db_error)}")
                database_success = False
        else:
            workflow_log.append("❌ Step 2 Skipped: Could not read CSV from bucket")
            database_success = False
        
        # ==========================================
        # STEP 3: Move to csv-imported folder (if successful)
        # ==========================================
        if database_success:
            workflow_log.append("\n📁 STEP 3: Moving file to csv-imported archive...")
            
            try:
                # Copy file to archive folder
                archive_file_path = f"{SUPABASE_ARCHIVE_FOLDER}/{unique_filename}"
                
                # Copy from csv-import to csv-imported
                copy_result = supabase.storage.from_(SUPABASE_BUCKET).copy(
                    from_path=import_file_path,
                    to_path=archive_file_path
                )
                
                workflow_log.append(f"✅ Step 3 Complete: Archived to {archive_file_path}")
                
                # ==========================================
                # STEP 4: Delete from csv-import folder
                # ==========================================
                workflow_log.append("\n🗑️ STEP 4: Cleaning up csv-import folder...")
                
                try:
                    delete_result = supabase.storage.from_(SUPABASE_BUCKET).remove([import_file_path])
                    workflow_log.append(f"✅ Step 4 Complete: Deleted {import_file_path}")
                    workflow_log.append("\n🎉 ALL STEPS COMPLETED SUCCESSFULLY!")
                    
                except Exception as delete_error:
                    workflow_log.append(f"⚠️ Step 4 Warning: Could not delete from csv-import - {str(delete_error)}")
                    workflow_log.append("💡 Manual cleanup may be required")
                
            except Exception as copy_error:
                workflow_log.append(f"⚠️ Step 3 Warning: Could not archive file - {str(copy_error)}")
                workflow_log.append("💡 File remains in csv-import folder")
                
        else:
            workflow_log.append("\n⚠️ STEP 3 & 4 SKIPPED: Database import failed")
            workflow_log.append(f"💡 File remains in csv-import folder: {import_file_path}")
        
        # ==========================================
        # Get public URL for final report
        # ==========================================
        try:
            public_url = supabase.storage.from_(SUPABASE_BUCKET).get_public_url(
                archive_file_path if database_success else import_file_path
            )
        except:
            public_url = "N/A"
        
        # ==========================================
        # Generate final report
        # ==========================================
        file_size_mb = len(file_content) / (1024 * 1024)
        success_rate = (inserted_count / len(df)) * 100 if len(df) > 0 else 0
        
        # Add helpful guidance for failed database imports
        huggingface_guidance = ""
        if not database_success and any("PGRST205" in log for log in workflow_log):
            huggingface_guidance = f"""

🚀 FOR HUGGING FACE DEPLOYMENT:
To fix table creation issues, create this table manually in your Supabase SQL Editor:

CREATE TABLE IF NOT EXISTS public.{table_name} (
    id SERIAL PRIMARY KEY,
    {", ".join([f'"{col}" TEXT' for col in df_clean.columns if col != 'created_date'])},
    created_date TIMESTAMP DEFAULT NOW()
);

Then re-upload your CSV file!
"""
        
        final_report = f"""
🔄 4-STEP WORKFLOW COMPLETED

📁 File Processing:
   • Original: {filename}
   • Unique ID: {unique_filename}
   • Size: {file_size_mb:.2f} MB
   • Rows: {len(df):,}
   • Columns: {len(df.columns)}

📊 Database Import:
   • Table: {table_name}
   • Success Rate: {success_rate:.1f}% ({inserted_count:,}/{len(df):,} rows)
   • Method: {method_used if database_success else 'Failed'}

📂 File Locations:
   • Original Location: {import_file_path}
   • Archive Location: {archive_file_path if database_success else 'N/A'}
   • Final Status: {'Archived & Cleaned' if database_success else 'Pending in csv-import'}

🔗 Access:
   • Public URL: {public_url}

{'🎉 WORKFLOW SUCCESSFUL!' if database_success else '⚠️ WORKFLOW INCOMPLETE - See details above'}{huggingface_guidance}

📝 Detailed Log:
{chr(10).join(workflow_log)}
"""
        
        return final_report
        
    except Exception as e:
        workflow_log.append(f"❌ CRITICAL ERROR: {str(e)}")
        return f"❌ Workflow failed: {str(e)}\n\n📝 Log:\n" + "\n".join(workflow_log)

def upload_csv_to_supabase(file):
    """Main Supabase upload function - now uses complete workflow"""
    return upload_csv_with_complete_workflow(file)

def handle_upload(file, destination):
    if file is None:
        return "❌ Error: Please select a file to upload"
    
    if destination == "AWS S3":
        return upload_csv_to_s3(file)
    elif destination == "Supabase":
        return upload_csv_to_supabase(file)
    else:
        return "❌ Error: Please select a destination"

with gr.Blocks() as demo:
    gr.Markdown("### Upload CSV to Cloud Storage")
    
    with gr.Group():
        gr.Markdown("#### Import Selection")
        destination_radio = gr.Radio(
            choices=["AWS S3", "Supabase"],
            value="AWS S3",
            label="Select Destination",
            info="Choose where to upload your CSV file"
        )
    
    file_input = gr.File(file_types=[".csv"], label="Upload CSV")
    upload_btn = gr.Button("Upload", variant="primary")
    output = gr.Textbox(label="Result")

    upload_btn.click(handle_upload, inputs=[file_input, destination_radio], outputs=output)

if __name__ == "__main__":
    demo.launch()
