# Big Data Project

Big data storage and processing (IT4931).

Group: 10

| No  | Full name               |      Student ID  |
|-----|-------------------------|------------------|
|  1  | Trần Duy Mẫn            |      20210566    |
|  2  | Hà Đức Chung            |      20215322    |
|  3  | Phạm Tiến Duy           |      20215335    |
|  4  | Phùng Thanh Đăng        |      20210150    |
|  5  | Nguyễn Trình Tuấn Đạt   |      20210177    |



# Bicycle Theft Analytics Pipeline

A comprehensive data pipeline solution for processing and analyzing bicycle theft data using Apache Airflow, Apache Spark, and HDFS.

## Architecture Overview

The project implements a modern data pipeline architecture with the following components:

- **Apache Airflow**: Orchestrates the entire data pipeline workflow
- **Apache Spark**: Handles distributed data processing and analytics
- **HDFS**: Stores raw and processed data
- **PostgreSQL**: Stores the final processed data
- **FastAPI**: Provides REST API for data ingestion

## System Requirements

- Docker and Docker Compose
- At least 16GB RAM recommended
- 4+ CPU cores recommended
- 20GB+ free disk space

## Project Structure

```
├── airflow/
│   ├── dags/
│   │   └── bicycle_data_pipeline.py
│   └── docker/
│       └── Dockerfile
├── spark/
│   ├── jobs/
│   │   ├── app.py
│   │   └── transformation.py
│   ├── jars/
│   └── Dockerfile
├── data_ingestion_service/
│   ├── src/
│   │   ├── main.py
│   │   └── utils.py
│   └── Dockerfile
├── hdfs/
│   ├── namenode/
│   └── datanode-{1,2,3}/
└── docker-compose.yml
```

## Setup Instructions

1. Clone the repository:
```bash
git clone [repository-url]
cd bicycle-theft-analytics
```

2. Create necessary directories:
```bash
mkdir -p airflow/logs spark/jars hdfs/namenode hdfs/datanode-{1,2,3}
```

3. Set up environment variables:
```bash
cp airflow/airflow.env.example airflow/airflow.env
```

4. Start the services:
```bash
docker-compose up -d
```

5. Verify all services are running:
```bash
docker-compose ps
```

## Component Access

- Airflow UI: http://localhost:8090
  - Username: admin
  - Password: admin
- Spark Master UI: http://localhost:8080
- HDFS UI: http://localhost:9870
- Data Ingestion API: http://localhost:8000

## Pipeline Workflow

1. **Data Ingestion**:
   - Raw data is sent to the data ingestion service via REST API
   - Data is validated and stored in HDFS

2. **Data Processing**:
   - Airflow triggers Spark jobs for data processing
   - Multiple transformations are applied:
     - Premise analysis
     - Temporal analysis
     - Division analysis
     - Neighborhood analysis
     - Security risk clustering
     - Monthly division summaries

3. **Data Storage**:
   - Processed data is stored in PostgreSQL
   - Results can be accessed via database queries

## Data Transformations

The pipeline performs several key transformations:

- Data cleaning and standardization
- Temporal analysis of theft patterns
- Geographic clustering and risk assessment
- Cost analysis by location and time
- Security risk level calculation using K-means clustering

## Configuration

### Airflow DAG Configuration

The main DAG (`bicycle_data_pipeline.py`) can be configured through the following parameters:

```python
default_args = {
    "depends_on_past": False,
    "start_date": datetime(2024, 10, 31),
    "email": ["your-email@example.com"],
    "retries": 1,
    "retry_delay": 5,
}
```

### Spark Configuration

Spark executors can be configured in `docker-compose.yml`:

```yaml
deploy:
  resources:
    limits:
      cpus: '2'
      memory: 1G
```

## Monitoring

- Monitor Airflow DAGs through the Airflow UI
- Check Spark job progress through the Spark Master UI
- Monitor HDFS storage and health through the HDFS UI

## Troubleshooting

Common issues and solutions:

1. **Services fail to start**:
   - Ensure sufficient system resources
   - Check logs: `docker-compose logs [service-name]`

2. **DAG failures**:
   - Check Airflow logs in the UI
   - Verify HDFS connectivity
   - Ensure Spark cluster is healthy

3. **Data processing issues**:
   - Check Spark UI for job details
   - Verify data format and schema
   - Check available resources

## Bash running
1. **To send data to HDFS you can do any of two ways below**
* Assess localhost 8080 to send data to HDFS using fast API docs
* access (fill date you want to send data to HDFS): http://localhost:8000/send_data_by_date?date={**date**}

2. **RUN spark to process data then send to Postgres DB**
```
docker exec -it spark-master bash -c "spark-submit --master spark://spark-master:7077 --jars /opt/spark/jars/postgresql-42.7.4.jar /opt/spark/app/app.py"
```
