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
from datetime import datetime
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
from airflow import DAG
from airflow.providers.http.hooks.http import HttpHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.dates import days_ago
from airflow.decorators import task
from pendulum import today
from datetime import datetime
from sklearn.preprocessing import LabelEncoder, MinMaxScaler










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
    'start_date':today('UTC').add(days=-1)

}


with DAG(dag_id='stock_pipeline', default_args=default_args, schedule='@daily', catchup=False) as dags:
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
    transformed_data=transform_data(stock_data)
    load(transformed_data)







    
    
    
    
    dataset_names = pd.read_csv("./report.csv", header=None)
    dataset_links = pd.read_csv("./links.csv", header=None)

    links = Tracker(url="https://www.moneycontrol.com/india/stockpricequote/")

    dataset_names = dataset_names.reset_index(drop=True)
    dataset_links = dataset_links.reset_index(drop=True)

    folder_path = "./new_folder"
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
folder_path = "./new_folder"
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
        file_path = f"./new_folder/{company_name}.json"
        
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
        
        csv_file_path = f"./new_folder/{company_name}.csv"
        
        with open(csv_file_path, 'w', newline='') as csv_file:
            fieldnames = ['t', 'ap', 'cp', 'v']
            writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
            writer.writeheader()
            
            for row in bse_data:
                try:
                    formatted_row = {
                        't': str(row.get('t', '')),  # Keep 't' as a string (if it's a timestamp)
                        'ap': float(row.get('ap', 0)),  # Convert to float, default to 0 if missing
                        'cp': float(row.get('cp', 0)),
                        'v': float(row.get('v', 0))
                    }
                    writer.writerow(formatted_row)
                except ValueError as e:
                    print(f"Skipping row with invalid values in {company_name}: {row} -> {e}")

        print(f"Data successfully written to {csv_file_path}")

def get_csv_filenames(folder_path):
    return [os.path.splitext(file)[0] for file in os.listdir(folder_path) if file.endswith('.csv')]


folder_path = './new_folder'
csv_filenames1 = get_csv_filenames(folder_path)


def model():
    st.subheader("Train the Model")

    # Get CSV files from folder
    csv_filenames1 = [f[:-4] for f in os.listdir(folder_path) if f.endswith('.csv')]

    if csv_filenames1:
        default_index = csv_filenames1.index("360onewam") if "360onewam" in csv_filenames1 else 0
        company_name = st.selectbox("Select the Company", csv_filenames1, index=default_index)
    else:
        st.error("No CSV files found in the folder.")
        st.stop()

    selected_file = f"{folder_path}/{company_name}.csv"

    if not os.path.exists(selected_file):
        st.error(f"File for {company_name} not found. Please check the file path.")
        return

    try:
        dataset = pd.read_csv(selected_file)

        if dataset.empty:
            st.error("Dataset is empty. Check the CSV file.")
            return

        # Convert 't' column (if present) to Unix timestamp
        if "t" in dataset.columns:
            dataset["t"] = pd.to_datetime(dataset["t"], errors='coerce').astype(int) // 10**9  # Convert to Unix

        # Handle categorical data
        for col in dataset.select_dtypes(include=['object']).columns:
            dataset[col] = LabelEncoder().fit_transform(dataset[col])

        # Handle missing values
        dataset.fillna(dataset.mean(), inplace=True)

        # Feature selection
        features = st.multiselect("Select feature columns", dataset.columns.tolist(), default=dataset.columns[:-1].tolist())
        target = st.selectbox("Select target column", dataset.columns.tolist(), index=len(dataset.columns) - 1)

        if st.button("Train Model"):
            X = dataset[features]
            y = dataset[target]

            # Normalize Features
            scaler = MinMaxScaler()
            X_scaled = scaler.fit_transform(X)

            # Train-test split
            X_train, X_test, y_train, y_test = train_test_split(X_scaled, y, test_size=0.2, random_state=42)

            # Train Random Forest
            regressor = RandomForestRegressor(n_estimators=200, max_depth=10, random_state=42)
            regressor.fit(X_train, y_train)
            st.session_state.regressor = regressor
            st.session_state.scaler = scaler  # Store scaler for later use

            # Model Evaluation
            y_pred = regressor.predict(X_test)
            mae = mean_absolute_error(y_test, y_pred)
            mse = mean_squared_error(y_test, y_pred)
            rmse = mse ** 0.5
            r2 = r2_score(y_test, y_pred)  # R² Score

            st.success("Model trained successfully!")
            st.write("### Model Evaluation")
            st.write(f"**Mean Absolute Error (MAE):** {mae:.2f}")
            st.write(f"**Mean Squared Error (MSE):** {mse:.2f}")
            st.write(f"**Root Mean Squared Error (RMSE):** {rmse:.2f}")
            st.write(f"**R² Score (Accuracy):** {r2:.4f}")  # Display accuracy

    except Exception as e:
        st.error(f"Error: {e}")

    # Prediction Section
    if "regressor" in st.session_state and st.session_state.regressor is not None:
        iter = st.number_input("Enter number of predictions needed", min_value=1, step=1, value=1)
        input_data_list = []

        for i in range(iter):
            inputs = {}
            if "t" in features:  # Only ask for timestamp if it's in the selected features
                unix_time = st.number_input(f"Enter Unix timestamp for prediction {i + 1}", min_value=0, key=f"timestamp_{i}")
                inputs["t"] = unix_time

            input_data_list.append(inputs)

        if st.button("Predict"):
            test_df = pd.DataFrame(input_data_list)

            # Normalize Input Data
            test_df_scaled = st.session_state.scaler.transform(test_df)

            predictions = st.session_state.regressor.predict(test_df_scaled)
            st.session_state.predictions = predictions
            st.write("### Predicted Prices")
            for i, pred in enumerate(predictions):
                st.write(f"Prediction {i + 1}: {pred:.2f}")
# get_chart_value(dataset_links=dataset_links)
# predict_price()


def main():
    st.title("Stock Price Prediction")
    st.sidebar.header("Options")

    menu = ["Predict Price", "Train Model", "Convert Timestamp"]
    choice = st.sidebar.selectbox("Select option", menu)

    if choice == "Predict Price":
        predict_price()

    if choice == "Train Model":
        model()

    if choice == "Convert Timestamp":
        user_date = st.text_input("Enter date (YYYY-MM-DD HH:MM:SS)", "")
        if st.button("Convert to Unix Timestamp"):
            try:
                unix_time = int(datetime.strptime(user_date, '%Y-%m-%d %H:%M:%S').timestamp())
                st.success(f"Unix Timestamp: {unix_time}")
            except ValueError:
                st.error("Invalid date format. Please enter in 'YYYY-MM-DD HH:MM:SS' format.")

if __name__ == "__main__":
    main()



