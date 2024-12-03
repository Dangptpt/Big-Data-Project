import os
import pandas as pd
from fastapi import FastAPI, BackgroundTasks
from hdfs import InsecureClient
from datetime import datetime
import time
from clean_data import clean_bicycle_data

app = FastAPI()

HDFS_URL = os.environ.get('HDFS_URL', 'http://hdfs-namenode:9000')
HDFS_USER = os.environ.get('HDFS_USER', 'root')

client = InsecureClient(HDFS_URL, user=HDFS_USER)

raw_data = pd.read_csv("data/Bicycle_Thefts.csv")
cleaned_data = clean_bicycle_data(raw_data)
cleaned_data["OCC_DATE"] = pd.to_datetime(cleaned_data["OCC_DATE"])
sorted_data = cleaned_data.sort_values(by="OCC_DATE")
batch_size = 100
batch_index = 0

def send_to_hdfs_in_batches():
    global batch_index
    while batch_index * batch_size < len(sorted_data):
        start = batch_index * batch_size
        end = min((batch_index + 1) * batch_size, len(sorted_data))
        batch_data = sorted_data.iloc[start:end]
        timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
        hdfs_path = f"/data/bicycle_thefts_batch_{batch_index}_{timestamp}.csv"
        
        try:
            with client.write(hdfs_path, encoding='utf-8') as writer:
                batch_data.to_csv(writer, index=False)
            print(f"Batch {batch_index} sent to HDFS successfully")
        except Exception as e:
            print(f"Error sending batch to HDFS: {e}")
        
        batch_index += 1
        time.sleep(3600)  # 1h/time 

@app.on_event("startup")
def schedule_hdfs_upload():
    import threading
    threading.Thread(target=send_to_hdfs_in_batches, daemon=True).start()

@app.get("/get_data")
def get_data():
    return {"message": "Dữ liệu đang được gửi theo từng batch lên HDFS."}

@app.get("/health")
def health_check():
    return {"status": "healthy"}