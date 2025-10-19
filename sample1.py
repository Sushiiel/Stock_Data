import os,re,csv,json,time
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import streamlit as st
from datetime import datetime,timedelta
from requests import get
from requests.exceptions import RequestException,ConnectionError,Timeout
from bs4 import BeautifulSoup
from sklearn.preprocessing import MinMaxScaler,LabelEncoder
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_absolute_error,mean_squared_error,r2_score
from matplotlib.ticker import MaxNLocator,FuncFormatter,ScalarFormatter

DATA_FOLDER="./new_folder"
REPORT_CSV="./report.csv"
LINKS_CSV="./links.csv"
JSON_LIST_CSV="./json_files.csv"
POSTGRES_CONN_ID="postgres_default"
API_CONN_ID="open_api"

class Tracker:
    def __init__(self,url):
        self.url=url
        self.user_agent={"User-Agent":"Mozilla/5.0 (Linux; Android 6.0; Nexus 5) AppleWebKit/537.36 (KHTML,like Gecko) Chrome/130.0.0.0 Mobile Safari/537.36"}
        self.soup=self.get_soup(url)
    def get_soup(self,url,retries=3,timeout=10):
        for _ in range(retries):
            try:
                r=get(url,headers=self.user_agent,timeout=timeout)
                r.raise_for_status()
                return BeautifulSoup(r.text,"lxml")
            except (ConnectionError,Timeout):
                time.sleep(1)
            except RequestException:
                break
        return None
    def product_price(self):
        if not self.soup: return "Price Not Found"
        p=self.soup.find("div",{"id":"nsecp"})
        return p.text if p else "Price Not Found"
    def extract_links(self,out_csv=LINKS_CSV):
        if not self.soup: return
        table=self.soup.find("table",{"class":"pcq_tbl MT10"})
        links=[a["href"] for a in table.find_all("a",href=True)] if table else []
        with open(out_csv,"w",newline="") as f:
            w=csv.writer(f); w.writerow(["Link"]); w.writerows([[x] for x in links])

def ensure_folder(p):
    if not os.path.exists(p): os.makedirs(p)

def Extract(url,out_csv=REPORT_CSV):
    try:
        r=get(url,timeout=10); r.raise_for_status()
        soup=BeautifulSoup(r.content,"lxml")
        table=soup.find("table",{"class":"pcq_tbl MT10"})
        rows=table.find_all("tr") if table else []
        with open(out_csv,"w",newline="",encoding="utf-8") as f:
            w=csv.writer(f)
            for tr in rows:
                cells=[td.text.strip().replace(" ","").replace("\n","").lower() for td in tr.find_all("td")]
                if cells:
                    for c in cells:
                        for v in c.split(","):
                            w.writerow([v])
    except RequestException:
        pass

def get_chart_value(dataset_links,folder_path=DATA_FOLDER,sleep_sec=2):
    ensure_folder(folder_path)
    data_dict={}
    for i in range(len(dataset_links)):
        link=dataset_links.iloc[i,0]
        if not isinstance(link,str): continue
        m=re.search(r"/([^/]+)/([^/]+)/([^/]+)$",link)
        if not m: continue
        company=m.group(2)
        tail=re.search(r"/([A-Za-z0-9]+)$",link)
        if not tail: continue
        scid=tail.group(1)
        url=f"https://www.moneycontrol.com/stocks/company_info/get_vwap_chart_data.php?classic=true&sc_did={scid}"
        try:
            r=get(url,timeout=15)
            if r.status_code==200:
                data=r.json()
                data_dict[company]=data
                with open(os.path.join(folder_path,f"{company}.json"),"w") as f:
                    json.dump({company:data},f,indent=2)
        except RequestException:
            continue
        time.sleep(sleep_sec)

def materialize_json_to_csv(folder_path=DATA_FOLDER):
    ensure_folder(folder_path)
    json_files=[f for f in os.listdir(folder_path) if f.endswith(".json")]
    for jf in json_files:
        company=os.path.splitext(jf)[0]
        p=os.path.join(folder_path,jf)
        try:
            with open(p,"r") as f: payload=json.load(f)
        except json.JSONDecodeError:
            continue
        data=list(payload.values())[0] if isinstance(payload,dict) else {}
        bse=data.get("stockpricequote",{}).get("BSE",[])
        if not isinstance(bse,list) or not bse: continue
        out=os.path.join(folder_path,f"{company}.csv")
        with open(out,"w",newline="") as f:
            w=csv.DictWriter(f,fieldnames=["t","ap","cp","v"])
            w.writeheader()
            for row in bse:
                try:
                    w.writerow({
                        "t":str(row.get("t","")),
                        "ap":float(row.get("ap",0)),
                        "cp":float(row.get("cp",0)),
                        "v":float(row.get("v",0))
                    })
                except ValueError:
                    continue

def list_company_csvs(folder_path=DATA_FOLDER):
    return sorted([f[:-4] for f in os.listdir(folder_path) if f.endswith(".csv")])

def _set_page_config():
    st.set_page_config(page_title="Stock Price Prediction",page_icon="📈",layout="wide")
    st.markdown("""
        <style>
        #MainMenu,footer{visibility:hidden}
        .metric-card{padding:16px;border-radius:16px;box-shadow:0 4px 24px rgba(0,0,0,.08);background:#fff;border:1px solid #eee}
        .section-card{padding:20px;border-radius:16px;background:#fff;border:1px solid #eee}
        .muted{color:#666;font-size:.9rem}
        </style>
    """,unsafe_allow_html=True)

def _persist_training_config(features,target,metrics=None,company_name=None):
    st.session_state["feature_names"]=list(features)
    st.session_state["target_name"]=target
    if metrics: st.session_state["last_train_metrics"]=metrics
    if company_name: st.session_state["last_trained_company"]=company_name

def _format_price_axis(ax):
    ax.yaxis.set_major_locator(MaxNLocator(nbins=6,prune="both"))
    sf=ScalarFormatter(useOffset=False)
    sf.set_scientific(False)
    ax.yaxis.set_major_formatter(sf)
    ax.grid(True,axis="y",alpha=.25)

def predict_price_ui(folder_path=DATA_FOLDER):
    st.title("🔮 Predict Stock Price")
    companies=list_company_csvs(folder_path)
    if not companies:
        st.warning("No CSVs found. Run data fetch first.")
        return
    c1,c2=st.columns([2,1])
    with c1: company=st.selectbox("Company",companies)
    with c2: n=st.number_input("Predictions to make",min_value=1,max_value=50,value=1,step=1)
    path=os.path.join(folder_path,f"{company}.csv")
    try: df=pd.read_csv(path)
    except Exception as e:
        st.error(f"Failed to load {path}: {e}")
        return
    df["t"]=pd.to_numeric(df["t"],errors="coerce"); df=df.dropna(subset=["t"])
    df["datetime"]=pd.to_datetime(df["t"],unit="s",errors="coerce")
    chart_df=df[["datetime","ap","cp"]].dropna().tail(100)
    m1,m2,m3,m4=st.columns(4)
    with m1:
        st.markdown('<div class="metric-card">',unsafe_allow_html=True)
        st.metric("Loaded Rows",f"{len(df):,}")
        st.markdown('</div>',unsafe_allow_html=True)
    with m2:
        st.markdown('<div class="metric-card">',unsafe_allow_html=True)
        st.metric("Chart Window",f"{len(chart_df):,} pts")
        st.markdown('</div>',unsafe_allow_html=True)
    with m3:
        st.markdown('<div class="metric-card">',unsafe_allow_html=True)
        st.metric("Model Trained","Yes ✅" if "regressor" in st.session_state else "No ❌")
        st.markdown('</div>',unsafe_allow_html=True)
    with m4:
        st.markdown('<div class="metric-card">',unsafe_allow_html=True)
        st.metric("Last Trained On",st.session_state.get("last_trained_company","—"))
        st.markdown('</div>',unsafe_allow_html=True)
    t1,t2=st.tabs(["📊 Overview","🎯 Predict"])
    with t1:
        st.subheader("Price Trend (last 100 points)")
        if chart_df.empty:
            st.info("Not enough data to draw.")
        else:
            fig,ax=plt.subplots(figsize=(10,4),constrained_layout=True)
            ax.plot(chart_df["datetime"],chart_df["ap"],label="Ask Price")
            ax.plot(chart_df["datetime"],chart_df["cp"],label="Current Price")
            ax.set_xlabel("Time"); ax.set_ylabel("Price"); ax.legend()
            _format_price_axis(ax)
            st.pyplot(fig)
        metrics=st.session_state.get("last_train_metrics")
        if metrics:
            st.markdown("#### Last Training Metrics")
            k1,k2,k3,k4=st.columns(4)
            k1.metric("MAE",f"{metrics['mae']:.2f}")
            k2.metric("MSE",f"{metrics['mse']:.2f}")
            k3.metric("RMSE",f"{metrics['rmse']:.2f}")
            k4.metric("R²",f"{metrics['r2']:.4f}")
        else:
            st.info("Train a model to see metrics.")
    with t2:
        st.subheader("Make Predictions")
        if "regressor" not in st.session_state or st.session_state.regressor is None:
            st.warning("No trained model found. Train a model first.")
            return
        feature_names=st.session_state.get("feature_names")
        target_name=st.session_state.get("target_name")
        if not feature_names or not target_name:
            st.error("Training configuration missing. Re-train the model.")
            return
        with st.form("prediction_form"):
            st.markdown('<div class="section-card">',unsafe_allow_html=True)
            rows=[]
            for idx in range(n):
                st.write(f"**Prediction {idx+1}**")
                cols=st.columns(min(4,len(feature_names)))
                values={}
                for j,feat in enumerate(feature_names):
                    with cols[j%len(cols)]:
                        if feat=="t":
                            d=st.date_input(f"Date #{idx+1}",key=f"d_{idx}")
                            t=st.time_input(f"Time #{idx+1}",key=f"time_{idx}")
                            values[feat]=int(datetime.combine(d,t).timestamp()) if d and t else int(pd.Timestamp.now().timestamp())
                        else:
                            default=float(df[feat].dropna().iloc[-1]) if feat in df.columns and not df[feat].dropna().empty else 0.0
                            values[feat]=st.number_input(f"{feat} #{idx+1}",value=float(default),key=f"{feat}_{idx}")
                rows.append(values)
            st.markdown('</div>',unsafe_allow_html=True)
            submitted=st.form_submit_button("Predict")
        if submitted:
            try:
                test_df=pd.DataFrame(rows,columns=feature_names)
                scaler=st.session_state.scaler
                model=st.session_state.regressor
                preds=model.predict(scaler.transform(test_df))
                out=test_df.copy(); out[target_name+"_pred"]=preds
                st.success("Predictions complete.")
                st.dataframe(out,use_container_width=True)
                fname=f"{company}_predictions_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
                out.to_csv(fname,index=False)
                with open(fname,"rb") as f:
                    st.download_button("Download predictions CSV",f,file_name=fname)
            except Exception as e:
                st.error(f"Prediction failed: {e}")
        st.markdown("---")
        st.write("**Batch Predictions (CSV Upload)**")
        st.code(", ".join(feature_names),language="text")
        up=st.file_uploader("Upload CSV",type=["csv"])
        if up is not None:
            try:
                up_df=pd.read_csv(up)
                missing=[c for c in feature_names if c not in up_df.columns]
                if missing:
                    st.error(f"Missing required columns: {missing}")
                else:
                    scaler=st.session_state.scaler
                    model=st.session_state.regressor
                    preds=model.predict(scaler.transform(up_df[feature_names]))
                    res=up_df.copy(); res[target_name+"_pred"]=preds
                    st.success("Batch predictions complete.")
                    st.dataframe(res.head(100),use_container_width=True)
                    fname=f"{company}_batch_predictions_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
                    res.to_csv(fname,index=False)
                    with open(fname,"rb") as f:
                        st.download_button("Download batch predictions CSV",f,file_name=fname)
            except Exception as e:
                st.error(f"Batch prediction failed: {e}")

def train_model_ui(folder_path=DATA_FOLDER):
    st.title("🛠️ Train Model")
    companies=list_company_csvs(folder_path)
    if not companies:
        st.error("No CSV files found.")
        return
    default_idx=companies.index("360onewam") if "360onewam" in companies else 0
    company=st.selectbox("Training Company",companies,index=default_idx)
    path=os.path.join(folder_path,f"{company}.csv")
    try: df=pd.read_csv(path)
    except Exception as e:
        st.error(f"Error loading file: {e}")
        return
    df["t"]=pd.to_numeric(df["t"],errors="coerce"); df=df.dropna(subset=["t"])
    df["datetime"]=pd.to_datetime(df["t"],unit="s",errors="coerce")
    for c in ["ap","cp","v"]:
        if c in df.columns: df[c]=pd.to_numeric(df[c],errors="coerce")
    with st.expander("Show Price Trend"):
        fig,ax=plt.subplots(figsize=(10,4),constrained_layout=True)
        ax.plot(df["datetime"],df["ap"],label="Ask Price")
        ax.plot(df["datetime"],df["cp"],label="Current Price")
        ax.set_xlabel("Date"); ax.set_ylabel("Price"); ax.legend()
        _format_price_axis(ax)
        st.pyplot(fig)
    raw=df.copy()
    if "t" in raw.columns and (np.issubdtype(raw["t"].dtype,np.datetime64) or str(raw["t"].dtype).startswith("datetime64")):
        raw["t"]=pd.to_datetime(raw["t"],errors="coerce").astype(int)//10**9
    for col in raw.select_dtypes(include=["object"]).columns:
        raw[col]=LabelEncoder().fit_transform(raw[col].astype(str))
    raw=raw.fillna(raw.mean(numeric_only=True))
    all_cols=[c for c in raw.columns if c!="datetime"]
    default_features=[c for c in all_cols if c!=all_cols[-1]]
    features=st.multiselect("Feature columns",all_cols,default=default_features)
    target=st.selectbox("Target column",[c for c in all_cols if c!="datetime"],index=len(all_cols)-1)
    if st.button("Train Model"):
        if not features:
            st.error("Select at least one feature."); return
        try:
            X=raw[features].values; y=raw[target].values
            scaler=MinMaxScaler(); Xs=scaler.fit_transform(X)
            Xtr,Xte,Ytr,Yte=train_test_split(Xs,y,test_size=.2,random_state=42)
            model=RandomForestRegressor(n_estimators=200,max_depth=10,random_state=42)
            model.fit(Xtr,Ytr)
            yhat=model.predict(Xte)
            mae=mean_absolute_error(Yte,yhat)
            mse=mean_squared_error(Yte,yhat)
            rmse=mse**.5
            r2=r2_score(Yte,yhat)
            st.session_state.regressor=model
            st.session_state.scaler=scaler
            st.session_state.model_accuracy=r2
            _persist_training_config(features,target,{"mae":mae,"mse":mse,"rmse":rmse,"r2":r2},company)
            st.success("Model trained successfully.")
            k1,k2,k3,k4,k5=st.columns(5)
            k1.metric("MAE",f"{mae:.2f}")
            k2.metric("MSE",f"{mse:.2f}")
            k3.metric("RMSE",f"{rmse:.2f}")
            k4.metric("R²",f"{r2:.4f}")
            k5.metric("Testing Accuracy",f"{r2:.4f}")
        except Exception as e:
            st.error(f"Training failed: {e}")

def main():
    _set_page_config()
    st.sidebar.header("Options")
    choice=st.sidebar.selectbox("Select option",["Predict Price","Train Model","Convert Timestamp"])
    if choice=="Predict Price": predict_price_ui()
    elif choice=="Train Model": train_model_ui()
    else:
        st.title("🕒 Convert Timestamp")
        s=st.text_input("Enter date (YYYY-MM-DD HH:MM:SS)","")
        if st.button("Convert to Unix Timestamp"):
            try:
                ts=int(datetime.strptime(s,"%Y-%m-%d %H:%M:%S").timestamp())
                st.success(f"Unix Timestamp: {ts}")
            except ValueError:
                st.error("Invalid format. Use YYYY-MM-DD HH:MM:SS")

try:
    from airflow import DAG
    from airflow.decorators import task
    from pendulum import today
    from airflow.providers.http.hooks.http import HttpHook
    from airflow.providers.postgres.hooks.postgres import PostgresHook
    default_args={"owner":"airflow","start_date":today("UTC").add(days=-1)}
    with DAG(dag_id="stock_pipeline",default_args=default_args,schedule="@daily",catchup=False) as dag:
        @task()
        def extract_data():
            scid=os.environ.get("SC_DID")
            if not scid: raise ValueError("SC_DID env var not set")
            http=HttpHook(http_conn_id=API_CONN_ID,method="GET")
            endpoint=f"/stocks/company_info/get_vwap_chart_data.php?classic=true&sc_did={scid}"
            r=http.run(endpoint)
            if r.status_code==200: return r.json()
            raise RuntimeError(f"Failed to fetch data: {r.status_code}")
        @task()
        def transform_data(stock_data):
            if "BSE" in stock_data:
                return [{"timestamp":i["t"],"actual_price":i["ap"],"closed_price":i["cp"],"volume":i["v"]} for i in stock_data["BSE"]]
            if "stockpricequote" in stock_data and "BSE" in stock_data["stockpricequote"]:
                return [{"timestamp":i["t"],"actual_price":i["ap"],"closed_price":i["cp"],"volume":i["v"]} for i in stock_data["stockpricequote"]["BSE"]]
            raise KeyError("BSE data not found")
        @task()
        def load_to_db(rows):
            pg=PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
            conn=pg.get_conn(); cur=conn.cursor()
            cur.execute("""
                CREATE TABLE IF NOT EXISTS company_prices(
                    timestamp BIGINT,
                    actual_price DOUBLE PRECISION,
                    closed_price DOUBLE PRECISION,
                    volume DOUBLE PRECISION
                )
            """)
            for r in rows:
                cur.execute(
                    "INSERT INTO company_prices(timestamp,actual_price,closed_price,volume) VALUES (%s,%s,%s,%s)",
                    (r["timestamp"],r["actual_price"],r["closed_price"],r["volume"])
                )
            conn.commit(); cur.close(); conn.close()
        load_to_db(transform_data(extract_data()))
except ImportError:
    pass

if __name__=="__main__":
    ensure_folder(DATA_FOLDER)
    if os.path.exists(REPORT_CSV) and os.path.exists(LINKS_CSV):
        try:
            names=pd.read_csv(REPORT_CSV,header=None)
            links=pd.read_csv(LINKS_CSV,header=None)
        except Exception:
            names=pd.DataFrame(); links=pd.DataFrame()
    else:
        names=pd.DataFrame(); links=pd.DataFrame()
    main()
