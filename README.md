
# **Big Data Project**

### **Course**: Lưu trữ và xử lý dữ liệu lớn (IT4931)  
### **Group**: 10  

| **No** | **Full Name**           | **Student ID**  |
|--------|--------------------------|-----------------|
| 1      | Trần Duy Mẫn            | 20210566        |
| 2      | Hà Đức Chung            | 20215322        |
| 3      | Phạm Tiến Duy           | 20215335        |
| 4      | Phùng Thanh Đăng        | 20210150        |
| 5      | Nguyễn Trình Tuấn Đạt   | 20210177        |

---

## **Project Overview**

This project leverages **Apache Spark**, **HDFS**, and **PostgreSQL** to process large-scale data. The architecture integrates **Airflow** for orchestration, **Docker** for containerization, and a custom-built FastAPI service for data ingestion into HDFS.

---

## **Setup Instructions**

### **1. Initial Setup**
Run the following command to build and start all services using Docker Compose:
```bash
docker-compose up --build -d
```

---

### **2. Run project without using Airflow**
1. **You can send data to HDFS using one of two methods:**

    **Access FastAPI Documentation:**
   - Open your browser at **`http://localhost:8000/docs`** to use the FastAPI interface.
   
    **Use Direct API Endpoint:**
   - Replace `{date}` with format: `YYYY-MM-DD`, example: `2015-10-20`, follow your desired date to send data:
     ```bash
     http://localhost:8000/send_data_by_date?date={date}
     ```


3. **Run Spark Job to Process Data**

    To process data and send results to the PostgreSQL database,
    execute the following command:
    ```bash
    docker exec -it spark-master bash -c "spark-submit --master
    spark://spark-master:7077 --jars /opt/spark/jars/postgresql
    42.7.4.jar /opt/spark/app/app.py"
    ```

---
### **3. Use airflow to run project**
1. **Use Airflow web UI to run**

    **Acess airflow WebUI address: `http://localhost:8090`**

    **Login with:**
    ```
    username: admin
    password: admin
    ```
    **Trigger dag to run.**
    
---

## **Project Configuration**

Below is the **Docker Compose Configuration** used in this project:

### **Shared Configuration**
```yaml
x-airflow-common: &airflow-common
  build:
    context: ./airflow/docker
    dockerfile: Dockerfile
  env_file:
    - ./airflow/airflow.env
  volumes:
    - airflow-logs:/opt/airflow/logs
    - ./airflow/dags:/opt/airflow/dags
    - ./spark/jars:/opt/spark/jars
    - ./spark/jobs:/opt/spark/app
  depends_on:
    - postgres
  networks:
    - airflow
```

### **Key Services**

1. **PostgreSQL Database**
   - Image: `postgres:13`
   - Port: `5432`
   - Database: `crime`

2. **Airflow**
   - Webserver and Scheduler:
     - Airflow Web UI accessible at **`http://localhost:8090`**
     - Pre-configured connection: `spark_default` for Spark.

3. **Apache Spark**
   - **Spark Master**:
     - Port: `7077` (Cluster mode)
     - Port: `8080` (Web UI)
   - **Spark Workers**:
     - 3 Workers with 1GB memory and 2 CPUs each.
     - Ports: `8081`, `8082`, `8083`.

4. **HDFS**
   - Namenode:
     - Accessible at **`http://localhost:9870`** (HDFS Web UI).
     - Core FS: `hdfs://hdfs-namenode:9000`
   - 3 Datanodes for redundancy and replication.

5. **Data Ingestion Service**
   - FastAPI Service running on **`http://localhost:8000`**.
   - Facilitates data ingestion into HDFS.

---

## **Volumes and Networks**

### **Volumes**
- **PostgreSQL Data**: `postgres-data`
- **Airflow Logs, DAGs, and Plugins**:
  - `airflow-logs`
  - `airflow-dags`
  - `airflow-plugins`
- **HDFS Namenode and Datanodes**:
  - `hdfs-namenode`
  - `hdfs-datanode-1`
  - `hdfs-datanode-2`
  - `hdfs-datanode-3`

### **Networks**
- **Bridge Network**: `airflow`

---

## **Important Links**
- **Airflow Web UI**: [http://localhost:8090](http://localhost:8090)
- **HDFS Namenode Web UI**: [http://localhost:9870](http://localhost:9870)
- **FastAPI Docs**: [http://localhost:8000/docs](http://localhost:8000/docs)

---

## **Team Contributions**
- **Codebase**: Distributed among all team members.
- **Testing**: Conducted collectively to ensure project reliability.
- **Documentation**: Compiled by all team members.
