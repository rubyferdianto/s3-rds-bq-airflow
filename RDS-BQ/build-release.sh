gcloud artifacts repositories create etl \
  --repository-format=docker --location=asia-southeast1 \
  --description="Meltano ETL images"

gcloud auth configure-docker asia-southeast1-docker.pkg.dev

gcloud builds submit --tag \
  asia-southeast1-docker.pkg.dev/infinite-byte-458600-a8/etl/meltano-aurora-to-bq:latest

gcloud secrets create AURORA_PASSWORD --data-file=-