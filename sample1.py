import requests
from bs4 import BeautifulSoup
import csv
import pandas as pd
import re
import json
import os
from tensorflow.keras.models import Sequential
from tensorflow.keras.layers import LSTM, Dense
from sklearn.preprocessing import MinMaxScaler
import streamlit as st
from requests.exceptions import RequestException, ConnectionError, Timeout
import time
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_absolute_error, mean_squared_error
from airflow import DAG
from airflow.providers.http.hooks.http import HttpHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.dates import days_ago
from airflow.decorators import task











class Tracker:
    def __init__(self, url):
        self.url = url
        self.user_agent = {
            "User-Agent": "Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) AppleWebKit/537.36 (KHTML,like Gecko) Chrome/130.0.0.0 Mobile Safari/537.36"
        }
        try:
            self.soup = self.get_soup(self.url)
        except RequestException as e:
            print(f"Error accessing URL {self.url}: {e}")
            self.soup = None

    def get_soup(self, url, retries=3, timeout=10):
        for attempt in range(retries):
            try:
                response = requests.get(url=url, headers=self.user_agent, timeout=timeout)
                response.raise_for_status()
                return BeautifulSoup(response.text, "lxml")
            except (ConnectionError, Timeout) as e:
                print(f"Error occurred on attempt {attempt + 1}: {e}. Retrying...")
            except RequestException as e:
                print(f"Failed to access {url}: {e}")
                break
        return None

    def product_price(self):
        if not self.soup:
            return "Price Not Found (Connection Error)"
        price = self.soup.find("div", {"id": "nsecp"})
        return price.text if price else "Price Not Found"

    def extract_links(self):
        if not self.soup:
            print("Unable to extract links (Connection Error).")
            return
        table = self.soup.find("table", {"class": "pcq_tbl MT10"})
        links = [a_tag["href"] for a_tag in table.find_all("a", href=True)] if table else []
        with open("links.csv", "w", newline="") as file:
            writer = csv.writer(file)
            writer.writerow(["Link"])
            writer.writerows([[link] for link in links])

POSTGRES_CONN_ID='postgres_default'
API_CONN_ID='open_api'
default_args={
    'owner':'airflow',
    'start_date':days_ago(1)

}


with DAG(dag_id='stock_pipeline', default_args=default_args, schedule_interval='@daily', catchup=False) as dags:
    @task()
    def extract_data():
        """Extract Stock price Data"""
        http_hook = HttpHook(http_conn_id=API_CONN_ID, method='GET')
        # https://www.moneycontrol.com

        endpoint = f"/stocks/company_info/get_vwap_chart_data.php?classic=true&sc_did={id}"
        response = http_hook.run(endpoint)

        if response.status_code == 200:
            return response.json()
        else:
            raise Exception(f"Failed to fetch weather data:{response.status_code}")

    
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
    def load(transformed_data):
        """Load Transformed data into postgresSQL"""
        pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        conn = pg_hook.get_conn()
        cursor = conn.cursor()

        cursor.execute("""
        CREATE TABLE IF NOT EXISTS Company1(
            TimeStamp INT,
            Actual_Price FLOAT,
            Closed_Price FLOAT,
            Value FLOAT);""")

        cursor.execute("""
        INSERT INTO Company1 (TimeStamp,Actual_Price,Closed_Price,Value) VALUES (%s,%s,%s,%s)
        """,(transform_data['timestamp'],
        transform_data['actual_price'],
        transform_data['closed_price'],
        transform_data['volume']))


        conn.commit()
        cursor.close()
        
        ## DAG workflow- ETL Pipeline
    stock_data=extract_data()
    transformed_data=transform_data(stock_data=stock_data)
    load(transformed_data)







    
    
    
    
    dataset_names = pd.read_csv("./report.csv", header=None)
    dataset_links = pd.read_csv("./links.csv", header=None)

    links = Tracker(url="https://www.moneycontrol.com/india/stockpricequote/")

    dataset_names = dataset_names.reset_index(drop=True)
    dataset_links = dataset_links.reset_index(drop=True)

    folder_path = "./Chart_values"
    if not os.path.exists(folder_path):
        os.makedirs(folder_path)
    
    
    # @task()
    # def get_chart_value(dataset_links):
    #     data_dict = {}
    #     for i in range(len(dataset_links)):
    #         url = dataset_links.iloc[i, 0]
    #         match = re.search(r'\/([^\/]+)\/([^\/]+)\/([^\/]+)$', url)
    #         if match:
    #             company_name = match.group(2)
    #             print(company_name)
    #             print("successful")

    #         json_file_path = os.path.join(folder_path, f'{company_name}.json')
    #         with open(json_file_path, 'w') as f:
    #             link = dataset_links.iloc[i, 0]
    #             if isinstance(link, str):
    #                 pattern = r"/([A-Za-z0-9]+)$"
    #                 match = re.search(pattern, link)
    #                 if match:
    #                     print("successful")
    #                     company_name = link.split("/")[4]
    #                     id = match.group(1)
    #                     url = f"https://www.moneycontrol.com/stocks/company_info/get_vwap_chart_data.php?classic=true&sc_did={id}"
    #                     response = requests.get(url)
    #                     time.sleep(3)
    #                     if response.status_code == 200:
    #                         print("successful")
    #                         data = response.json()
    #                         data_dict[company_name] = data
    #                         json.dump(data_dict, f, indent=4)
    #                         data_dict = {}
    #         with open(json_file_path, 'r') as json_file:
    #             data = json.load(json_file)
    #         return data

    @task()
    def load():
        pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        conn = pg_hook.get_conn()
        cursor = conn.cursor()
        json_files = [f[:-5] for f in os.listdir(folder_path) if f.endswith('.json')]
        csv_file_path = 'json_files.csv'
        df = pd.DataFrame({'file_name': json_files})
        df.to_csv(csv_file_path, index=False)
        df_from_csv = pd.read_csv(csv_file_path)

        for company_name in df_from_csv['file_name']:
            json_file_path = os.path.join(folder_path, f'{company_name}.json')
            if not os.path.exists(json_file_path):
                continue
            with open(json_file_path, 'r') as json_file:
                company_data = json.load(json_file)
            bse_data = company_data.get("stockpricequote", {}).get("BSE", [])
            if not bse_data or not isinstance(bse_data, list):
                continue
            table_name = f"sample_{company_name}"
            cursor.execute(f"""
                CREATE TABLE IF NOT EXISTS {table_name}(
                    TimeStamp INT,
                    Actual_Price FLOAT,
                    Closed_Price FLOAT,
                    Value FLOAT
                )
            """)
            for record in bse_data:
                timestamp = record.get("t")
                actual_price = record.get("ap")
                closed_price = record.get("cp")
                value = record.get("v")
                cursor.execute(f"""
                    INSERT INTO {table_name} (TimeStamp, Actual_Price, Closed_Price, Value)
                    VALUES (%s, %s, %s, %s)
                """, (timestamp, actual_price, closed_price, value))
            conn.commit()
        conn.close()






def Extract(url):
    try:
        response = requests.get(url=url, timeout=10)
        response.raise_for_status()
        soup = BeautifulSoup(response.content, 'lxml')
        table = soup.find("table", {"class": "pcq_tbl MT10"})
        trow = table.find_all("tr") if table else []
        with open("report.csv", "w", newline='', encoding='utf-8') as csv_file:
            csv_write = csv.writer(csv_file)
            for tr in trow:
                cells = [td.text.strip().replace(' ', '').replace('\n', '').lower() for td in tr.find_all("td")]
                if cells:
                    for cell in cells:
                        split_values = cell.split(',')
                        for value in split_values:
                            csv_write.writerow([value])
    except RequestException as e:
        print(f"Error extracting data from URL {url}: {e}")


dataset_names = pd.read_csv("./report.csv", header=None)
dataset_links = pd.read_csv("./links.csv", header=None)
links = Tracker(url="https://www.moneycontrol.com/india/stockpricequote/")
dataset_names = dataset_names.reset_index(drop=True)
dataset_links = dataset_links.reset_index(drop=True)
folder_path = "./Chart_values"
if not os.path.exists(folder_path):
    os.makedirs(folder_path)


def get_chart_value(dataset_links):
    data_dict = {}
    for i in range(len(dataset_links)):
        url = dataset_links.iloc[i, 0]
        match = re.search(r'\/([^\/]+)\/([^\/]+)\/([^\/]+)$', url)
        if match:
            company_name = match.group(2)
            print(company_name)
            print("successful")


            
        json_file_path = os.path.join(folder_path, f'{company_name}.json')
        with open(json_file_path, 'w') as f:
            link = dataset_links.iloc[i, 0]
            if isinstance(link, str):
                pattern = r"/([A-Za-z0-9]+)$"
                match = re.search(pattern, link)
                if match:
                    print("successful")
                    company_name = link.split("/")[4]
                    id = match.group(1)
                    url = f"https://www.moneycontrol.com/stocks/company_info/get_vwap_chart_data.php?classic=true&sc_did={id}"
                    response = requests.get(url)
                    time.sleep(3)
                    if response.status_code == 200:
                        print("successful")
                        data = response.json()
                        data_dict[company_name] = data
                        json.dump(data_dict, f, indent=4)
                        data_dict = {}

json_files = [f[:-5] for f in os.listdir(folder_path) if f.endswith('.json')]

csv_file_path = 'json_files.csv'
df = pd.DataFrame({'file_name': json_files})
df.to_csv(csv_file_path, index=False)

df_from_csv = pd.read_csv(csv_file_path)


def predict_price():
    for i in range(len(df_from_csv)):
        company_name = df_from_csv.iloc[i, 0]
        file_path = f"./Chart_values/{company_name}.json"
        if not os.path.exists(file_path):
            continue
        try:
            with open(file_path, 'r') as json_file:
                company_data = json.load(json_file)
        except json.JSONDecodeError:
            print(f"Error decoding JSON for {company_name}. Skipping...")
            continue

        bse_data = company_data.get("stockpricequote", {}).get("BSE", [])
        if not bse_data or not isinstance(bse_data, list):
            continue
        csv_file_path = f"./Chart_values/{company_name}.csv"
        with open(csv_file_path, 'a', newline='') as csv_file:
            writer = csv.DictWriter(csv_file, fieldnames=['t', 'ap', 'cp', 'v'])
            writer.writeheader()
            writer.writerows(bse_data)

        print(f"Data successfully written to {csv_file_path}")


def get_csv_filenames(folder_path):
    return [os.path.splitext(file)[0] for file in os.listdir(folder_path) if file.endswith('.csv')]


folder_path = './Chart_values'
csv_filenames1 = get_csv_filenames(folder_path)


def model():
    st.subheader("Train the Model")
    selected_file = None
    company_name = st.selectbox("Select the Company", csv_filenames1)
    selected_file = f"{folder_path}/{company_name}.csv"

    if os.path.exists(selected_file):
        try:
            dataset = pd.read_csv(selected_file)
            if not dataset.empty:
                features = st.multiselect("Select feature columns", dataset.columns.tolist(), default=dataset.columns[:-1].tolist())
                target = st.selectbox("Select target column", dataset.columns.tolist(), index=len(dataset.columns) - 1)

                if st.button("Train Model"):
                    X = dataset[features]
                    y = dataset[target]

                    # Train-Test Split
                    from sklearn.model_selection import train_test_split
                    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

                    # Train Random Forest Regressor
                    regressor = RandomForestRegressor(n_estimators=100, random_state=42)
                    regressor.fit(X_train, y_train)
                    st.session_state.regressor = regressor

                    # Evaluate Model
                    y_pred = regressor.predict(X_test)
                    mae = mean_absolute_error(y_test, y_pred)
                    mse = mean_squared_error(y_test, y_pred)
                    rmse = mse ** 0.5

                    st.success("Model trained successfully!")
                    st.write("### Model Evaluation")
                    st.write(f"**Mean Absolute Error (MAE):** {mae:.2f}")
                    st.write(f"**Mean Squared Error (MSE):** {mse:.2f}")
                    st.write(f"**Root Mean Squared Error (RMSE):** {rmse:.2f}")
        except Exception as e:
            st.error(f"Error: {e}")
    else:
        st.error(f"File for {company_name} not found. Please check the file path.")

    if "regressor" in st.session_state and st.session_state.regressor is not None:
        if "input_data_list" not in st.session_state:
            st.session_state.input_data_list = []

        iter = st.number_input("Enter number of predictions needed", min_value=1, step=1, value=1)
        for i in range(iter):
            inputs = {}
            for feature in features:
                inputs[feature] = st.number_input(f"Enter value for {feature} for prediction {i + 1}", key=f"{feature}_{i}")
            if len(st.session_state.input_data_list) < iter:
                st.session_state.input_data_list.append(inputs)

        if st.button("Predict"):
            test_df = pd.DataFrame(st.session_state.input_data_list[:iter])
            predictions = st.session_state.regressor.predict(test_df)
            st.session_state.predictions = predictions
            st.write("Predicted Prices:")
            for i, pred in enumerate(predictions):
                st.write(f"Prediction {i + 1}: {pred}")
get_chart_value(dataset_links=dataset_links)
# predict_price()


def main():
    st.title("Stock Price Prediction")
    st.sidebar.header("Options")

    menu = ["Predict Price", "Train Model"]
    choice = st.sidebar.selectbox("Select option", menu)

    if choice == "Predict Price":
        predict_price()

    if choice == "Train Model":
        model()


if __name__ == "__main__":
    main()



