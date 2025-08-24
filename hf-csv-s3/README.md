# CSV Upload to Supabase via Gradio

A Gradio web application for uploading CSV files to cloud storage (AWS S3 or Supabase) with automatic database import.

## Features

- 🚀 **Dual Cloud Support**: Upload to AWS S3 or Supabase Storage
- 📊 **Automatic Database Import**: CSV data imported to Supabase PostgreSQL
- 🔄 **4-Step Workflow**: Upload → Import → Archive → Cleanup
- 📅 **Auto Timestamps**: Adds `created_date` column automatically
- 🛠️ **Dynamic Table Creation**: Guides users to create tables for new CSV structures

## Quick Start

1. **Configure Environment Variables**:
   ```bash
   # Copy and edit .env file with your credentials
   SUPABASE_URL=your_supabase_url
   SUPABASE_KEY=your_supabase_key
   AWS_ACCESS_KEY_ID=your_aws_key
   AWS_SECRET_ACCESS_KEY=your_aws_secret
   ```

2. **Install Dependencies**:
   ```bash
   pip install -r requirements.txt
   ```

3. **Run the App**:
   ```bash
   python app.py
   ```

4. **Access**: Open http://localhost:7860

## For New CSV Files

If uploading a new CSV structure, the app will guide you to create the table:

```sql
CREATE TABLE IF NOT EXISTS public.your_table_name (
    id SERIAL PRIMARY KEY,
    -- your CSV columns will be listed here
    created_date TIMESTAMP DEFAULT NOW()
);
```

## Deployment

Ready for Hugging Face Spaces deployment with environment variables configured.
