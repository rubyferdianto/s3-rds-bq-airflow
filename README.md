# Requirements:
1. New database AWS RDS
2. New User > New Custom Policy
3. New S3 > Bucket

# Steps
1. Python will get the CSV files in local folder then import into S3.
2. Meltano will get from AWS S3 to AWS RDS
3. AWS RDS will transfer into GCP BigQuery
4. BigQuery process data analytic for factsales dimension
5. Visualisation get the factsales
6. Airflow will be used to monitor the process from steps 1 to 5

