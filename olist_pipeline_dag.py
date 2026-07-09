from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.email import EmailOperator
from datetime import datetime, timedelta
import boto3
import time

# ── Default args ──────────────────────────────────────────────
default_args = {
    'owner': 'neha',
    'depends_on_past': False,
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

BUCKET = 'list-ecommerce-neha'
CLUSTER_ID = 'j-WGY4O34018LJ'
REGION = 'eu-north-1'

# ── Task 1: Check S3 Bronze layer has data ────────────────────
def check_s3_data():
    s3 = boto3.client('s3', region_name=REGION)
    response = s3.list_objects_v2(Bucket=BUCKET, Prefix='bronze/')
    count = response.get('KeyCount', 0)
    if count < 9:
        raise ValueError(f"Expected 9 files in bronze/, found {count}")
    print(f"✅ S3 Bronze layer check passed: {count} files found")

# ── Task 2: Submit PySpark job to EMR ─────────────────────────
def submit_emr_job():
    emr = boto3.client('emr', region_name=REGION)
    response = emr.add_job_flow_steps(
        JobFlowId=CLUSTER_ID,
        Steps=[{
            'Name': 'olist-etl-scheduled',
            'ActionOnFailure': 'CONTINUE',
            'HadoopJarStep': {
                'Jar': 'command-runner.jar',
                'Args': [
                    'spark-submit',
                    '--deploy-mode', 'cluster',
                    f's3://{BUCKET}/scripts/transform.py'
                ]
            }
        }]
    )
    step_id = response['StepIds'][0]
    print(f"✅ EMR step submitted: {step_id}")

    # Wait for completion
    while True:
        steps = emr.list_steps(ClusterId=CLUSTER_ID, StepIds=[step_id])
        state = steps['Steps'][0]['Status']['State']
        print(f"   Step status: {state}")
        if state == 'COMPLETED':
            print("✅ EMR job completed successfully!")
            break
        elif state in ['FAILED', 'CANCELLED']:
            raise Exception(f"EMR step failed with state: {state}")
        time.sleep(30)

# ── Task 3: Refresh Athena partitions ─────────────────────────
def refresh_athena():
    client = boto3.client('athena', region_name=REGION)
    response = client.start_query_execution(
        QueryString='MSCK REPAIR TABLE olist_db.orders',
        QueryExecutionContext={'Database': 'olist_db'},
        ResultConfiguration={
            'OutputLocation': f's3://{BUCKET}/athena-results/'
        }
    )
    qid = response['QueryExecutionId']
    while True:
        status = client.get_query_execution(
            QueryExecutionId=qid
        )['QueryExecution']['Status']['State']
        if status == 'SUCCEEDED':
            print("✅ Athena partitions refreshed!")
            break
        elif status in ['FAILED', 'CANCELLED']:
            raise Exception(f"Athena refresh failed: {status}")
        time.sleep(5)

# ── Task 4: Check for late shipment spike ─────────────────────
def check_late_shipments():
    client = boto3.client('athena', region_name=REGION)
    response = client.start_query_execution(
        QueryString="""
            SELECT SUM(is_late) as late_count,
                   COUNT(*) as total,
                   ROUND(SUM(is_late) * 100.0 / COUNT(*), 2) as late_rate
            FROM olist_db.orders
            WHERE order_year = 2018 AND order_month = 8
        """,
        QueryExecutionContext={'Database': 'olist_db'},
        ResultConfiguration={
            'OutputLocation': f's3://{BUCKET}/athena-results/'
        }
    )
    qid = response['QueryExecutionId']
    while True:
        result = client.get_query_execution(QueryExecutionId=qid)
        status = result['QueryExecution']['Status']['State']
        if status == 'SUCCEEDED':
            break
        time.sleep(5)

    rows = client.get_query_results(
        QueryExecutionId=qid
    )['ResultSet']['Rows']

    if len(rows) > 1:
        late_rate = float(rows[1]['Data'][2].get('VarCharValue', 0))
        print(f"✅ Late shipment rate: {late_rate}%")
        if late_rate > 10:
            raise Exception(
                f"🚨 ALERT: Late shipment rate is {late_rate}% — exceeds 10% threshold!"
            )
    print("✅ Late shipment check passed")

# ── DAG definition ─────────────────────────────────────────────
with DAG(
    dag_id='olist_ecommerce_pipeline',
    default_args=default_args,
    description='Brazilian E-Commerce ETL Pipeline — daily run',
    schedule='0 0 * * *',  # Every day at midnight
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=['data-engineering', 'aws', 'pyspark', 'olist'],
) as dag:

    t1 = PythonOperator(
        task_id='check_s3_bronze_layer',
        python_callable=check_s3_data,
    )

    t2 = PythonOperator(
        task_id='submit_pyspark_job_to_emr',
        python_callable=submit_emr_job,
    )

    t3 = PythonOperator(
        task_id='refresh_athena_partitions',
        python_callable=refresh_athena,
    )

    t4 = PythonOperator(
        task_id='check_late_shipment_threshold',
        python_callable=check_late_shipments,
    )

    # Task dependencies — runs in sequence
    t1 >> t2 >> t3 >> t4
