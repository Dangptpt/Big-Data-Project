from fastapi import FastAPI, HTTPException, BackgroundTasks
import pandas as pd
from datetime import datetime
from hdfs import InsecureClient
import os

app = FastAPI()

HDFS_URL = os.environ.get('HDFS_URL', 'http://hdfs-namenode:9000')
HDFS_USER = os.environ.get('HDFS_USER', 'root')
client = InsecureClient(HDFS_URL, user=HDFS_USER)

raw_data = pd.read_csv("data/Bicycle_Thefts.csv")
cleaned_data = raw_data  
cleaned_data["OCC_DATE"] = pd.to_datetime(cleaned_data["OCC_DATE"])
sorted_data = cleaned_data.sort_values(by="OCC_DATE")


def send_to_hdfs_in_background(filtered_data: pd.DataFrame, hdfs_path: str):
    try:
        with client.write(hdfs_path, encoding="utf-8") as writer:
            filtered_data.to_csv(writer, index=False)
        print(f"Data successfully sent to HDFS at {hdfs_path}")
    except Exception as e:
        print(f"Error sending data to HDFS: {e}")


@app.post("/send_data_by_date")
def send_data_by_date(
    date: str, 
    background_tasks: BackgroundTasks
):
    global sorted_data

    try:
        query_date = pd.to_datetime(date)
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid date format. Use yyyy-mm-dd.")
    
    if sorted_data["OCC_DATE"].dtype == 'O': 
        sorted_data["OCC_DATE"] = pd.to_datetime(sorted_data["OCC_DATE"])
    
    filtered_data = sorted_data[
        sorted_data["OCC_DATE"].dt.date == query_date.date()
    ]

    if len(filtered_data) == 0:
        raise HTTPException(status_code=404, detail=f"No data found for the specified date: {date}.")

    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
    hdfs_path = f"/data/bicycle_thefts_{query_date.strftime('%Y%m%d')}_{timestamp}.csv"

    background_tasks.add_task(send_to_hdfs_in_background, filtered_data, hdfs_path)

    return {
        "message": f"Data for date {date} is being sent to HDFS in the background.",
        "date": date,
        "hdfs_path": hdfs_path,
    }
