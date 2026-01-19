# s3-2-redshift-airflow
Using Docker to build an Airflow image I built an ETL DAG that pulls data from a Redshift database and uploads it to S3 for working then pulls the file and performs different transformations to create two separate versions and uploads those to s3 for staging then copies to Redshift tables. 
