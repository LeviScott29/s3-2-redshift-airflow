# Redshift to S3 ETL Pipeline with Apache Airflow

An Apache Airflow data pipeline that extracts data from Amazon Redshift, performs transformations using Pandas, stages intermediate files in Amazon S3, and loads transformed datasets back into Redshift.

The project demonstrates building a lightweight **ETL workflow using Airflow DAGs, AWS services, and Python-based transformations**.

---

# Features

- Extracts data from Amazon Redshift using Airflow's `PostgresHook`
- Stores intermediate datasets as CSV files
- Uploads extracted data to Amazon S3
- Performs transformations using Pandas
- Creates multiple transformed datasets from a single source
- Loads processed files back into Redshift using the `COPY` command
- Uses Airflow task dependencies to orchestrate pipeline execution

---

# Project Structure
```
s3-2-redshift-airflow/
│
├── dags/
│ └── redshift_extraction.py # Airflow DAG defining ETL workflow
│
├── docker-compose.yml # Airflow container environment
├── env.env # Environment variables for services
├── .gitignore
└── README.md
```

---

# Architecture Overview
```
Pipeline workflow:
Redshift
│
▼
Extract data using Airflow + PostgresHook
│
▼
Save dataset locally as CSV
│
▼
Upload dataset to Amazon S3
│
▼
Transform data using Pandas
│
▼
Upload transformed files back to S3
│
▼
Load transformed data back into Redshift using COPY
```

---

# DAG Workflow

The Airflow DAG defines a pipeline with multiple tasks:

1. **redshift_to_s3**
   - Extracts data from Redshift
   - Writes data to CSV
   - Uploads file to S3

2. **transform1_to_s3**
   - Reads extracted file from S3
   - Renames and drops columns
   - Uploads transformed dataset to S3

3. **transform2_to_s3**
   - Reads extracted file
   - Performs alternative transformation
   - Uploads new dataset to S3

4. **s3_to_redshift1**
   - Truncates destination table
   - Loads first transformed dataset

5. **s3_to_redshift2**
   - Loads second transformed dataset

Task dependencies:
```
redshift_to_s3
│
├── transform1_to_s3 → s3_to_redshift1
│
└── transform2_to_s3 → s3_to_redshift2
```

---

# Technologies Used

- Python
- Apache Airflow
- Amazon Redshift
- Amazon S3
- Pandas
- Boto3
- Docker

---
