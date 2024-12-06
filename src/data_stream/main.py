import os
import pandas as pd
from fastapi import FastAPI, HTTPException, BackgroundTasks
from hdfs import InsecureClient
from datetime import datetime

app = FastAPI()

HDFS_URL = os.environ.get('HDFS_URL', 'http://hdfs-namenode:9000')
HDFS_USER = os.environ.get('HDFS_USER', 'root')

client = InsecureClient(HDFS_URL, user=HDFS_USER)

raw_data = pd.read_csv("data/Bicycle_Thefts.csv")
# cleaned_data = clean_bicycle_data(raw_data)
cleaned_data = raw_data
cleaned_data["OCC_DATE"] = pd.to_datetime(cleaned_data["OCC_DATE"])
sorted_data = cleaned_data.sort_values(by="OCC_DATE")
batch_size = 100
batch_index = 0

def send_to_hdfs_in_background(batch_index: int):
    global sorted_data, batch_size
    start = batch_index * batch_size
    end = min((batch_index + 1) * batch_size, len(sorted_data))

    if start >= len(sorted_data):
        print("All batches have been sent to HDFS.")
        return

    batch_data = sorted_data.iloc[start:end]
    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
    hdfs_path = f"/data/bicycle_thefts_batch_{batch_index}_{timestamp}.csv"

    try:
        with client.write(hdfs_path, encoding='utf-8') as writer:
            batch_data.to_csv(writer, index=False)
        print(f"Batch {batch_index} sent to HDFS successfully.")
    except Exception as e:
        print(f"Error sending batch {batch_index} to HDFS: {e}")

@app.post("/send_batch")
def send_batch(background_tasks: BackgroundTasks):
    global batch_index
    if batch_index * batch_size >= len(sorted_data):
        return {"message": "All batches have been sent to HDFS."}
    
    # Schedule the task in the background
    background_tasks.add_task(send_to_hdfs_in_background, batch_index)
    response = {
        "message": f"Batch {batch_index} is being sent to HDFS in the background.",
        "batch_index": batch_index,
    }
    batch_index += 1
    return response

@app.get("/health")
def health_check():
    return {"status": "healthy"}
