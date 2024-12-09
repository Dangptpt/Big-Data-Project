from pyspark.sql import SparkSession
from config import Config

class DataImporter:
    def __init__(self, host=None, port=None, dbname=None, user=None, password=None) -> None:
        config = Config()
        self.jdbc_url = f"jdbc:postgresql://{host or config.HOST}:{port or config.PORT}/{dbname or config.DATABASE}"
        self.user = user or config.USER
        self.password = password or config.PASSWORD

    def import_data(self, df, table_name):
        df.write \
            .format("jdbc") \
            .option("url", self.jdbc_url) \
            .option("dbtable", table_name) \
            .option("user", self.user) \
            .option("password", self.password) \
            .option("driver", "org.postgresql.Driver") \
            .mode("overwrite") \
            .save()
