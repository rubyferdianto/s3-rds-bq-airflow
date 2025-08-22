# Security Configuration Guide

## Environment Variables Setup

This project now uses environment variables to store sensitive credentials instead of hardcoding them in configuration files.

### Quick Setup

1. **Copy the example environment file:**
   ```bash
   cp .env.example .env
   ```

2. **Edit `.env` with your actual credentials:**
   ```bash
   nano .env  # or use your preferred editor
   ```

3. **Verify `.env` is ignored by Git:**
   ```bash
   git check-ignore .env  # Should output: .env
   ```

## 🔒 Security Best Practices

### What's Protected by .gitignore
- `.env` - Environment variables with secrets
- `google_credentials.json` - Google Cloud service account keys  
- `*.json` - Any JSON credential files

### Safe Credential Storage Options

#### Option 1: Environment Variables (Current Setup)
```bash
# In .env file
GOOGLE_CLOUD_CREDENTIALS_JSON='{"type": "service_account", "project_id": "your-project", ...}'
TAP_MYSQL_PASSWORD='your_password'
TARGET_BIGQUERY_PROJECT='your-project-id'
TARGET_BIGQUERY_DATASET='your_dataset'
```

#### Option 2: Credentials File (Alternative)
1. Create `google_credentials.json` with service account key
2. File is automatically ignored by git
3. Update `meltano.yml` to use `credentials_path` instead of `credentials_json`

## 🚫 Security Warnings

- **NEVER** commit credential files to git
- **NEVER** hardcode credentials in configuration files  
- **ALWAYS** regenerate credentials if accidentally exposed
- **REGULARLY** rotate service account keys

## 🔄 Key Management

1. **Generate new credentials** in Google Cloud Console
2. **Update local `.env` file** with new credentials
3. **Test the pipeline** to ensure credentials work
4. **Delete old credentials** from Google Cloud Console
   git status  # .env should not appear in untracked files
   ```

### Environment Variables Reference

#### MySQL Database
- `TAP_MYSQL_PASSWORD`: Password for MySQL database connection

#### Google Cloud BigQuery
- `GOOGLE_CLOUD_CREDENTIALS_JSON`: Complete service account JSON as a single string (recommended approach)

#### BigQuery Configuration
- `TARGET_BIGQUERY_PROJECT`: BigQuery project ID
- `TARGET_BIGQUERY_DATASET`: Target dataset name

### Setting Up Google Cloud Credentials

1. **Download your service account JSON file from Google Cloud Console**
2. **Convert the JSON to a single line string** (remove newlines except within the private key)
3. **Set the `GOOGLE_CLOUD_CREDENTIALS_JSON` environment variable** with the complete JSON string

Example:
```bash
GOOGLE_CLOUD_CREDENTIALS_JSON='{"type": "service_account", "project_id": "your-project", ...}'
```

### Security Benefits

✅ **Credentials no longer exposed in version control**  
✅ **Easy to rotate credentials without code changes**  
✅ **Different credentials per environment (dev/staging/prod)**  
✅ **Follows security best practices**  

### Loading Environment Variables

The environment variables are automatically loaded by Meltano when it starts. Make sure your `.env` file is in the same directory as `meltano.yml`.

### Troubleshooting

If you get authentication errors:
1. Verify all environment variables are set correctly in `.env`
2. Check that there are no extra spaces or quotes in the values
3. Ensure the private key includes proper newline characters (`\n`)
4. Verify the service account has the necessary BigQuery permissions

### Important Notes

- **Never commit the `.env` file to version control**
- **Use `.env.example` as a template for new setups**
- **Rotate credentials regularly**
- **Use different service accounts for different environments**
