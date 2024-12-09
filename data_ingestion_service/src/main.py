# data_ingestion_service/src/main.py
from fastapi import FastAPI, HTTPException, BackgroundTasks
import pandas as pd
from datetime import datetime
import os
from .utils import send_to_hdfs

app = FastAPI()

# Lấy biến môi trường
HDFS_URL = os.environ.get('HDFS_URL', 'http://hdfs-namenode:9000')
HDFS_USER = os.environ.get('HDFS_USER', 'root')

# Đọc dữ liệu từ CSV
raw_data = pd.read_csv("src/data/Bicycle_Thefts.csv")
cleaned_data = raw_data.copy()
cleaned_data["OCC_DATE"] = pd.to_datetime(cleaned_data["OCC_DATE"], errors='coerce')
sorted_data = cleaned_data.sort_values(by="OCC_DATE").dropna(subset=["OCC_DATE"])

def send_to_hdfs_in_background(filtered_data: pd.DataFrame, hdfs_path: str):
    send_to_hdfs(filtered_data, HDFS_URL, HDFS_USER, hdfs_path)

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
    
    # Kiểm tra và chuyển đổi kiểu dữ liệu nếu cần
    if sorted_data["OCC_DATE"].dtype == 'O': 
        sorted_data["OCC_DATE"] = pd.to_datetime(sorted_data["OCC_DATE"], errors='coerce')
        sorted_data = sorted_data.dropna(subset=["OCC_DATE"])
    
    # Lọc dữ liệu theo ngày
    filtered_data = sorted_data[
        sorted_data["OCC_DATE"].dt.date == query_date.date()
    ]

    if len(filtered_data) == 0:
        # raise HTTPException(status_code=404, detail=f"No data found for the specified date: {date}.")
        return False

    # Tạo đường dẫn HDFS với timestamp
    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
    hdfs_path = f"/data/bicycle_thefts_{query_date.strftime('%Y%m%d')}_{timestamp}.csv"

    # Thêm task gửi dữ liệu vào HDFS
    background_tasks.add_task(send_to_hdfs_in_background, filtered_data, hdfs_path)

    return {
        "message": f"Data for date {date} is being sent to HDFS in the background.",
        "date": date,
        "hdfs_path": hdfs_path,
    }
