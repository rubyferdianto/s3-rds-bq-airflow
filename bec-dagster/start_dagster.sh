#!/bin/bash
# Dagster Pipeline Launcher
# Usage: ./start_dagster.sh

echo "🚀 Starting Dagster S3-RDS-BigQuery Pipeline..."
echo "==========================================================="

# Check if we're in the DAGSTER directory
if [ ! -f "dagster_pipeline.py" ]; then
    echo "❌ Error: dagster_pipeline.py not found!"
    echo "Please run this script from the DAGSTER directory"
    exit 1
fi

# Initialize conda for bash
eval "$(conda shell.bash hook)"

# Activate bec environment
echo "🔧 Activating 'bec' conda environment..."
conda activate bec || {
    echo "❌ Failed to activate 'bec' environment"
    echo "💡 Please ensure conda environment 'bec' exists"
    exit 1
}

echo "🔍 Using Python: $(which python)"
echo "🔍 Python version: $(python --version)"
echo "🔍 Current environment: $CONDA_DEFAULT_ENV"

# Check if required packages are available
python -c "import dagster; print('✅ Dagster available')" || {
    echo "❌ Dagster not found in current environment"
    echo "💡 Please ensure Dagster is installed in 'bec' environment"
    exit 1
}

python -c "import boto3; print('✅ boto3 available')" || {
    echo "❌ boto3 not found in current environment"
    echo "💡 Installing boto3..."
    pip install boto3
}

echo "🌐 Starting Dagster development server..."
echo "📍 URL: http://127.0.0.1:3000"
echo "⏹️  Press Ctrl+C to stop"
echo ""

# Start Dagster
python -m dagster dev --host 127.0.0.1 --port 3000
