# data_ingestion_service/src/utils.py
from hdfs import InsecureClient
import pandas as pd

def send_to_hdfs(filtered_data: pd.DataFrame, hdfs_url: str, hdfs_user: str, hdfs_path: str):
    client = InsecureClient(hdfs_url, user=hdfs_user)
    try:
        with client.write(hdfs_path, encoding="utf-8") as writer:
            filtered_data.to_csv(writer, index=False)
        print(f"Data successfully sent to HDFS at {hdfs_path}")
    except Exception as e:
        print(f"Error sending data to HDFS: {e}")
