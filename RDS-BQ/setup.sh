#!/bin/bash
"""
Setup script for RDS to BigQuery pipeline
"""

echo "🚀 Setting up RDS to BigQuery pipeline..."

# Check if we're in the right directory
if [ ! -f "meltano.yml" ]; then
    echo "❌ Error: meltano.yml not found. Please run this script from the RDS-BQ directory."
    exit 1
fi

# Check if meltano is installed
if ! command -v meltano &> /dev/null; then
    echo "📦 Installing meltano..."
    pip install meltano
fi

# Install other requirements
echo "📦 Installing Python requirements..."
pip install -r ../requirements.txt

# Add extractors and loaders
echo "🔧 Adding Meltano plugins..."
meltano add extractor tap-mysql --variant=transferwise
meltano add loader target-bigquery --variant=z3z1ma

# Install plugins
echo "🔧 Installing Meltano plugins..."
meltano install

echo "✅ Setup complete!"
echo ""
echo "Next steps:"
echo "1. Make sure your .env file has the correct values"
echo "2. Set up Google Cloud authentication:"
echo "   - Download service account JSON file"
echo "   - Set GOOGLE_APPLICATION_CREDENTIALS in .env"
echo "   - OR run: gcloud auth application-default login"
echo "3. Run the pipeline: python run-pipeline.py"
