import requests
from bs4 import BeautifulSoup
import csv
import pandas as pd
import json
import os
import time
from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.dates import days_ago
from airflow.decorators import task
from airflow.operators.empty import EmptyOperator
import pendulum

POSTGRES_CONN_ID = 'postgres_default'
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'start_date': pendulum.today('UTC').add(days=-1),
}

BASE_URL = "https://www.moneycontrol.com"

with DAG(dag_id='stock_pipeline', start_date=pendulum.today('UTC').add(days=-1), default_args=default_args, schedule='@daily', catchup=False) as dag:
    
    start_task = EmptyOperator(task_id='start')
    end_task = EmptyOperator(task_id='end')
    
    @task()
    def extract_data():
        """Extract Stock price Data via Web Scraping"""
        stock_id = "12345"
        endpoint = f"{BASE_URL}/stocks/company_info/get_vwap_chart_data.php?classic=true&sc_did={stock_id}"
        response = requests.get(endpoint)
        
        if response.status_code == 200:
            return response.json()
        else:
            raise Exception(f"Failed to fetch stock data: {response.status_code}")
    
    @task()
    def transform_data(stock_data):
        """
        Transform Stock Data: Extract specific fields (t, cp, ap, v)
        """
        if "BSE" in stock_data:
            transformed_data = [
                {"timestamp": item["t"], "actual_price": item["ap"], "closed_price": item["cp"], "volume": item["v"]}
                for item in stock_data["BSE"]
            ]
            return transformed_data
        else:
            raise KeyError("Key 'BSE' not found in the stock data.")
    
    @task()
    def load_data(transformed_data):
        """Load Transformed data into PostgreSQL"""
        pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        conn = pg_hook.get_conn()
        cursor = conn.cursor()

        cursor.execute("""
        CREATE TABLE IF NOT EXISTS Company1 (
            TimeStamp INT,
            Actual_Price FLOAT,
            Closed_Price FLOAT,
            Value FLOAT
        );""")

        for data in transformed_data:
            cursor.execute("""
            INSERT INTO Company1 (TimeStamp, Actual_Price, Closed_Price, Value) 
            VALUES (%s, %s, %s, %s)
            """, (data['timestamp'], data['actual_price'], data['closed_price'], data['volume']))
        
        conn.commit()
        cursor.close()
        conn.close()

    # Setting up task dependencies
    start_task >> extract_data() >> transform_data(extract_data()) >> load_data(transform_data(extract_data())) >> end_task









