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
