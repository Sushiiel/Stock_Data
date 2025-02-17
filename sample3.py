from airflow import DAG
from airflow.providers.http.hooks.http import HttpHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.dates import days_ago
from airflow.decorators import task
import requests

POSTGRES_CONN_ID = 'postgres_default'
API_CONN_ID = 'open_api'
default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1)
}

with DAG(dag_id='stock_pipeline', default_args=default_args, schedule_interval='@daily', catchup=False) as dag:

    @task()
    def extract_data():
        """Extract Stock price Data"""
        http_hook = HttpHook(http_conn_id=API_CONN_ID, method='GET')
        endpoint = "/stocks/company_info/get_vwap_chart_data.php?classic=true&sc_did={id}"  # Replace {id} with actual value
        response = http_hook.run(endpoint)

        if response.status_code == 200:
            return response.json()
        else:
            raise Exception(f"Failed to fetch stock data: {response.status_code}")

    @task()
    def transform_data(stock_data):
        """Transform Stock Data: Extract specific fields (t, cp, ap, v)"""
        if "BSE" in stock_data:
            transformed_data = [
                {"timestamp": item["t"], "actual_price": item["ap"], "closed_price": item["cp"], "volume": item["v"]}
                for item in stock_data["BSE"]
            ]
            return transformed_data
        else:
            raise KeyError("Key 'BSE' not found in the stock data.")

    @task()
    def load(transformed_data):
        """Load Transformed data into PostgreSQL"""
        pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        conn = pg_hook.get_conn()
        cursor = conn.cursor()

        cursor.execute("""
        CREATE TABLE IF NOT EXISTS Company1 (
            TimeStamp INT,
            Actual_Price FLOAT,
            Closed_Price FLOAT,
            Volume FLOAT);
        """)

        # Insert data into the table
        for data in transformed_data:
            cursor.execute("""
            INSERT INTO Company1 (TimeStamp, Actual_Price, Closed_Price, Volume)
            VALUES (%s, %s, %s, %s)
            """, (data['timestamp'], data['actual_price'], data['closed_price'], data['volume']))

        conn.commit()
        cursor.close()

    # DAG workflow - ETL Pipeline
    stock_data = extract_data()
    transformed_data = transform_data(stock_data)
    load(transformed_data)
