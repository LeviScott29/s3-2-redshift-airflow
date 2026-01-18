from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import pandas as pd
import boto3


def redshift_to_s3():
    hook = PostgresHook(postgres_conn_id='<SECRETS_ID>')
    df = hook.get_pandas_df("SELECT * FROM <Table_name>;")
    df.to_csv('/tmp/myfile.csv', index=False)
    s3 = boto3.client("s3")
    s3.upload_file(Filename= '/tmp/myfile.csv', 
                   Bucket= '<BUCKET_ID>', 
                   Key = 'myfile.csv')
    return 'myfile.csv'

def transform1_to_s3(**context):
    s3= boto3.client("s3")
    path = context['ti'].xcom_pull(task_ids='redshift_to_s3')
    obj =  s3.get_object(Bucket = '<BUCKET_ID>', Key = path)
    df = pd.read_csv(obj['Body'])
    df.rename(columns ={'phone_1':'home_phone', 'phone_2': 'business_phone'}, inplace = True)
    print(df.columns)
    df.drop(columns =['<column_name'], inplace=True)
    df.to_csv('/tmp/myfile1.csv', index =False)
    s3.upload_file(Filename = '/tmp/myfile1.csv', 
                   Bucket ='<BUCKET_ID>', 
                   Key ='myfile1.csv')


def transform2_to_s3(**context):
    s3 = boto3.client("s3")
    path = context['ti'].xcom_pull(task_ids = 'redshift_to_s3')
    obj = s3.get_object(Bucket = '<BUCKET_ID>', Key = path)
    df = pd.read_csv(obj['Body'])
    df.rename(columns ={'phone_1':'business_phone', 'phone_2': 'home_phone'}, inplace =True)
    df.to_csv('/tmp/myfile2.csv', index = False)
    s3.upload_file(Filename = '/tmp/myfile2.csv', 
                   Bucket ='<BUCKET_ID>', 
                   Key = 'myfile2.csv')

def file1_to_redshift():
    s3 = boto3.client('s3')
    truncate_sql="TRUNCATE TABLE <Table_Name>"
    copy_sql="""
    COPY <Table_Name>
    FROM '<BUCKET_SOURCE>'
    IAM_ROLE <REDSHIFT_IAM_ROLE_ARN>'
    CSV
    IGNOREHEADER 1;
    """
    hook = PostgresHook(postgres_conn_id = '<SECRETS_ID>')
    hook.run(truncate_sql)
    hook.run(copy_sql)

def file2_to_redshift():
    s3=boto3.client('s3')
    truncate_sql="TRUNCATE TABLE <Table_Name>"
    copy_sql = """
     COPY stage.output2
    FROM '<BUCKET_SOURCE>'
    IAM_ROLE '<REDSHIFT_IAM_ROLE_ARN'
    CSV
    IGNOREHEADER 1;
    """
    hook = PostgresHook(postgres_conn_id = '<SECRETS_ID>S')
    hook.run(truncate_sql)
    hook.run(copy_sql)

    
default_args ={
    'owner' : 'levi',
    'depends_on_past' : False,
    'email' : ['somebody@gmail.com'],
    'email_on_failure' : False,
    'email_on_retry' : False,
    'retries' : 0,
    'retry_delay': timedelta(minutes=5)
}
with DAG(
    'my_redshift_dag',
    default_args = default_args,
    description = "pulls data from redshift then perform some transformations and returns it",
    schedule= None,
    start_date=datetime(2025, 12, 9),
    catchup = False,
    tags = ['testing'],
) as dag:
    
    task1 = PythonOperator(
        task_id = "redshift_to_s3",
        python_callable =redshift_to_s3
    )
    task2 = PythonOperator(
        task_id = "local_to_s3-1",
        python_callable = transform1_to_s3
    )
    task3 = PythonOperator(
        task_id = "local_to_s3-2",
        python_callable = transform2_to_s3
    )
    task4 = PythonOperator(
        task_id = "s3_to_redshift1",
        python_callable = file1_to_redshift
    )
    task5 = PythonOperator(
        task_id = "s3_to_redshift2",
        python_callable = file2_to_redshift
    )

    task1 >> [task2, task3]
    task2 >> [task4]
    task3 >> [task5]


