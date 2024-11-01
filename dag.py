from datetime import datetime, timedelta
import os
os.environ["PATH"] = f"/usr/local/airflow/.local/bin:{os.environ['PATH']}"
import tempfile
import boto3
import requests
import json
from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.models import Variable
s3_bucket = Variable.get("S3_BUCKET")
data_source_url = Variable.get("DATA_SOURCE_URL")

'''
This code is doing the following:

Importing modules for working with temporary files, accessing AWS, making web requests and parsing JSON
Importing modules required for defining a DAG and its components
Creating two Apache Airflow variables for the S3 bucket name and the data source URL
'''

def fetch_url_data(url, bucket, key):
    s3_client = boto3.client("s3")
    temp_file = tempfile.mkstemp()[1]
    resp = requests.get(url)
    with open(temp_file, "w") as fd:
        json.dump(resp.json(), fd)
    s3_client.upload_file(temp_file, bucket, key)
    os.remove(temp_file)
def fetch_s3_data(client, key, bucket):
    temp_file = tempfile.mkstemp()[1]
    client.download_file(bucket, key, temp_file)
    data = None
    with open(temp_file) as fd:
        data = json.load(fd)
    os.remove(temp_file)
    return data
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2015, 6, 1),
    "email": ["airflow@airflow.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


'''
above code is to define two helper functions and a dictionary for configuring a DAG
'''

@dag(
    default_args=default_args,
    schedule_interval="@monthly",
    start_date=days_ago(1),
)
def combine_and_enrich_dag():
    @task
    def get_customer_data():
        url = f"{data_source_url}/customers"
        fetch_url_data(url, s3_bucket, "data/customers.json")
    @task
    def get_order_data():
        url = f"{data_source_url}/orders"
        fetch_url_data(url, s3_bucket, "data/orders.json")
    @task
    def get_review_data():
        url = f"{data_source_url}/reviews"
        fetch_url_data(url, s3_bucket, "data/reviews.json")
    @task.virtualenv(
        requirements=["boto3==1.24.16", "textblob==0.17.1"],
        system_site_packages=False,
    )
    def analyse_reviews(s3_bucket):
        from textblob import TextBlob
        import tempfile, boto3, os, json
        s3_client = boto3.client("s3")
        temp_file = tempfile.mkstemp()[1]
        s3_client.download_file(s3_bucket, "data/reviews.json", temp_file)
        data = None
        with open(temp_file) as fd:
            data = json.load(fd)
        os.remove(temp_file)
        analysed_data = []
        for review in data:
            analysis = TextBlob(review["review"]).sentiment
            review["sentiment"] = analysis
            analysed_data.append(review)
        temp_file = tempfile.mkstemp()[1]
        with open(temp_file, "w") as fd:
            json.dump(analysed_data, fd)
        s3_client.upload_file(temp_file, s3_bucket, "data/reviews.json")
        os.remove(temp_file)
    @task
    def combine_data():
        s3_client = boto3.client("s3")
        customers = fetch_s3_data(s3_client, "data/customers.json", s3_bucket)
        orders = fetch_s3_data(s3_client, "data/orders.json", s3_bucket)
        reviews = fetch_s3_data(s3_client, "data/reviews.json", s3_bucket)
        combined_customer_data = []
        for customer in customers:
            combined = dict(customer)
            combined["orders"] = list([
                o for o in orders if customer["customer_id"] == o["customer_id"]
            ])
            order_ids = [o["order_id"] for o in combined["orders"]]
            combined["reviews"] = list([r for r in reviews if r["order_id"] in order_ids])
            combined_customer_data.append(dict(combined))
        temp_file = tempfile.mkstemp()[1]
        with open(temp_file, "w") as fd:
            json.dump(combined_customer_data, fd)
        s3_client.upload_file(temp_file, s3_bucket, "data/combined.json")
        os.remove(temp_file)
    (
        [get_customer_data(), get_review_data(), get_order_data()]
        >> analyse_reviews(s3_bucket)
        >> combine_data()
    )
dag = combine_and_enrich_dag()

'''above code is to define dag'''


