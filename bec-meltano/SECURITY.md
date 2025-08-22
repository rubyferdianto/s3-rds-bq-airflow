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
   git status  # .env should not appear in untracked files
   ```

### Environment Variables Reference

#### MySQL Database
- `TAP_MYSQL_PASSWORD`: Password for MySQL database connection

#### Google Cloud BigQuery
- `GOOGLE_CLOUD_PROJECT_ID`: Your GCP project ID
- `GOOGLE_CLOUD_PRIVATE_KEY_ID`: Service account private key ID
- `GOOGLE_CLOUD_PRIVATE_KEY`: Service account private key (with newlines)
- `GOOGLE_CLOUD_CLIENT_EMAIL`: Service account email
- `GOOGLE_CLOUD_CLIENT_ID`: Service account client ID
- `GOOGLE_CLOUD_AUTH_URI`: OAuth2 auth URI (usually standard)
- `GOOGLE_CLOUD_TOKEN_URI`: OAuth2 token URI (usually standard)
- `GOOGLE_CLOUD_AUTH_PROVIDER_X509_CERT_URL`: Provider cert URL (usually standard)
- `GOOGLE_CLOUD_CLIENT_X509_CERT_URL`: Client cert URL
- `GOOGLE_CLOUD_UNIVERSE_DOMAIN`: Universe domain (usually googleapis.com)

#### BigQuery Configuration
- `TARGET_BIGQUERY_PROJECT`: BigQuery project ID
- `TARGET_BIGQUERY_DATASET`: Target dataset name

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
