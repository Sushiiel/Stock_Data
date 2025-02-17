import requests
from bs4 import BeautifulSoup
import csv
import pandas as pd
import re
import json
import os
import numpy
from sklearn.ensemble import RandomForestRegressor
import streamlit as st

class Tracker:
    def __init__(self, url):
        self.url = url
        self.user_agent = {
            "User-Agent": "Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Mobile Safari/537.36"
        }
        self.response = requests.get(url=self.url, headers=self.user_agent).text
        self.soup = BeautifulSoup(self.response, "lxml")

    def product_price(self):
        price = self.soup.find("div", {"id": "nsecp"})
        return price.text if price else "Price Not Found"

    def extract_links(self):
        table = self.soup.find("table", {"class": "pcq_tbl MT10"})
        links = [a_tag["href"] for a_tag in table.find_all("a", href=True)] if table else []
        with open("links.csv", "w", newline="") as file:
            writer = csv.writer(file)
            writer.writerow(["Link"])
            writer.writerows([[link] for link in links])

    

    def get_data(self,price):
        self.response=requests.get(url=self.url).content
        self.soup=BeautifulSoup(self.response,"lxml")
        company_details=self.soup.find_all()




def Extract(url):
    response = requests.get(url=url).content
    soup = BeautifulSoup(response, 'lxml')
    table = soup.find("table", {"class": "pcq_tbl MT10"})
    trow = table.find_all("tr") if table else []
    with open("report.csv", "w", newline='', encoding='utf-8') as csv_file:
        csv_write = csv.writer(csv_file)
        for tr in trow:
            cells = [
                td.text.strip().replace(' ', '').replace('\n', '').lower()
                for td in tr.find_all("td")
            ]
            if cells:
                for cell in cells:
                    split_values = cell.split(',')
                    for value in split_values:
                        csv_write.writerow([value])

# Extract(url="https://www.moneycontrol.com/india/stockpricequote/")
dataset_names = pd.read_csv("/Users/raj/Desktop/Stock_Data/report.csv", header=None)
dataset_links = pd.read_csv("/Users/raj/Desktop/Stock_Data/links.csv", header=None)

links = Tracker(url="https://www.moneycontrol.com/india/stockpricequote/")
# links.extract_links()

dataset_names = dataset_names.reset_index(drop=True)
dataset_links = dataset_links.reset_index(drop=True)


def get_price():
    for i in range(len(dataset_names)):
        url = dataset_links.iloc[i, 0]
        if url == "Link" or not url.startswith("http"):
            print(f"Skipping invalid URL at index {i}: {url}")
            continue
        tracker = Tracker(url=url)
        price = tracker.product_price()
        print(f"Price for {dataset_names.iloc[i, 0]}: {price}")


def get_single():
    wanted=[]
    company_name=input("Enter the company name").strip().lower()
    for i in range(len(dataset_names)):
        if company_name in dataset_names.iloc[i,0]:
            wanted.append(company_name)
        
    for i in range(len(dataset_links)):
        flag=dataset_links.iloc[i,0]
        if f"/{company_name}/" in flag:
            index=i
            break
    
    return wanted[0],index

folder_path = "/Users/raj/Desktop/Stock_Data/Chart_values"
if not os.path.exists(folder_path):
    os.makedirs(folder_path)


def get_chart_value():
    data_dict = {}
    
    for i in range(len(dataset_links)):
            url=dataset_links.iloc[i,0]
            match = re.search(r'\/([^\/]+)\/([^\/]+)\/([^\/]+)$', url)
            if match:
                company_name = match.group(2)
            json_file_path = os.path.join(folder_path, f'{company_name}.json')
            with open(json_file_path, 'w') as f:
                link = dataset_links.iloc[i, 0]
                if isinstance(link, str):
                    pattern = r"/([A-Za-z0-9]+)$"
                    match = re.search(pattern, link)
                    if match:
                        company_name = link.split("/")[4]
                        id = match.group(1)
                        url = f"https://www.moneycontrol.com/stocks/company_info/get_vwap_chart_data.php?classic=true&sc_did={id}"
                        
                        response = requests.get(url)
                        if response.status_code == 200:
                            data = response.json()
                            data_dict[company_name] = data
                            json.dump(data_dict, f,indent=4)
                            data_dict = {}

def get_price_single():
    company_name,company_index=get_single()
    url = dataset_links.iloc[company_index,0]
    if url == "Link" or not url.startswith("http"):
        print(f"Skipping invalid URL at index {company_index}: {url}")
    tracker = Tracker(url=url)
    price = tracker.product_price()
    print(f"Price for {company_name}:{price}")

def get_data(price1):
    for i in range(len(dataset_names)):
        url = dataset_links.iloc[i, 0]
        if url == "Link" or not url.startswith("http"):
            print(f"Skipping invalid URL at index {i}: {url}")
            continue
        tracker = Tracker(url=url)
        price2 = tracker.product_price()
        price = float(price2.replace(',', ''))
        if price<price1:
            print(dataset_names.iloc[i,0])

# get_data(200)
# get_price()
# get_price_single()
get_chart_value()
fnp=0
scf=0
nv=0

folder_path = '/Users/raj/Desktop/Stock_Data/Chart_values'
json_files = [f[:-5] for f in os.listdir(folder_path) if f.endswith('.json')]

csv_file_path = 'json_files.csv'
df = pd.DataFrame({'file_name': json_files})
df.to_csv(csv_file_path, index=False)

df_from_csv = pd.read_csv(csv_file_path)
# print(df_from_csv)


def predict_price():
    for i in range(len(df_from_csv)):
        company_name = df_from_csv.iloc[i,0]
        file_path = f"/Users/raj/Desktop/Stock_Data/Chart_values/{company_name}.json"
        
        if not os.path.exists(file_path):
            print("File not present")
            continue

        with open(file_path, 'r') as json_file:
            company_data = json.load(json_file)

        bse_data = company_data.get("stockpricequote", {}).get("BSE", [])

        if not bse_data or not isinstance(bse_data, list):
            print("No valid data found in the JSON file.")
            continue

        csv_file_path = f"/Users/raj/Desktop/Stock_Data/Chart_values/{company_name}.csv"

        with open(csv_file_path, 'w', newline='') as csv_file:
            writer = csv.DictWriter(csv_file, fieldnames=['t', 'ap', 'cp', 'v'])
            writer.writeheader()
            writer.writerows(bse_data)
        
        # print(f"Data successfully written to {csv_file_path}")

        
def model():
    dataset1=pd.read_csv('/Users/raj/Desktop/Stock_Data/Chart_values/360onewam.csv')
    X=dataset1.drop(['cp','v'],axis=1)
    Y=dataset1['cp']
    model=RandomForestRegressor(n_estimators=100,random_state=42)
    model.fit(X,Y)
    iter=int(input("enter number of predictions needed"))
    input_data_list=[]
    for i in range(iter):
        t=int(input("enter the timestanmp"))
        ap=float(input("enter the actual price"))
        input_data_list.append({'t': t, 'ap': ap})
    

    test1=pd.DataFrame(input_data_list)
    output1=model.predict(test1)
    print(output1)

# model()
# predict_price()
