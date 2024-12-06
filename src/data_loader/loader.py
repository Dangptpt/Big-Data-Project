from pyspark.sql import SparkSession

class DataImporter:
    def __init__(self, host, port, dbname, user, password) -> None:
        self.jdbc_url = f"jdbc:postgresql://{host}:{port}/{dbname}"
        self.user = user
        self.password = password
    
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