from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from pyspark.sql.types import *
from pyspark.sql.functions import col, to_timestamp

class DataTransformationInit:
    def __init__(self) -> None:
        self.spark = SparkSession.builder \
            .appName("CrimeAnalytics") \
            .getOrCreate()
        
    def load_and_prepare_data(self, input_path):
        df = self.spark.read.csv(input_path, header=True, inferSchema=True)
        date_columns = ['OCC_DATE', 'REPORT_DATE']
        for column in date_columns:
            df = df.withColumn(column, to_timestamp(column, 'M/d/yyyy h:mm:ss a'))

        df = df.withColumn("BIKE_COST", col("BIKE_COST").cast("double")) \
            .withColumn("BIKE_SPEED", col("BIKE_SPEED").cast("integer")) \
            .withColumn("LAT_WGS84", col("LAT_WGS84").cast("double")) \
            .withColumn("LONG_WGS84", col("LONG_WGS84").cast("double"))

        return df
    
    def process_temporal_data(self, df):
        temporal_df = (
            df.groupBy("OCC_HOUR", "OCC_DOW", "OCC_YEAR")
            .count()
            .withColumnRenamed("count", "total_cases")
            .select(
                col("total_cases").alias("count"),
                col("OCC_HOUR").alias("hour"),
                col("OCC_DOW").alias("day_of_week"),
                col("OCC_YEAR").alias("year")
            )
        )
        return temporal_df
    
    def process_division_data(self, df):
        # Group by DIVISION, PREMISES_TYPE, and OCC_YEAR to calculate total cases
        division_yearly = (
            df.groupBy("DIVISION", "PREMISES_TYPE", "OCC_YEAR")
            .count()
            .withColumnRenamed("count", "total_cases")
        )

        # Calculate average total cases for each DIVISION and PREMISES_TYPE across all years
        average_by_division = (
            division_yearly.groupBy("DIVISION", "PREMISES_TYPE")
            .agg(avg("total_cases").alias("average_cases"))
        )

        # Join yearly data with average data to compute % change
        division_df = (
            division_yearly.join(average_by_division, on=["DIVISION", "PREMISES_TYPE"])
            .withColumn("change", (col("total_cases") - col("average_cases")) / col("average_cases") * 100)
            .select(
                col("DIVISION").alias("division"),
                col("PREMISES_TYPE").alias("premise_type"),
                col("total_cases").alias("count"),
                col("OCC_YEAR").alias("year")
            )
        )

        return division_df
    
    def process_neighbourhood_data(self, df):
        # Group by PREMISES_TYPE, OCC_YEAR, and OCC_MONTH to calculate total cases
        premises_monthly = (
            df.groupBy("PREMISES_TYPE", "OCC_YEAR", "OCC_MONTH")
            .count()
            .withColumnRenamed("count", "total_cases")
        )
        
        # Join monthly data with average yearly data to compute % change (only based on year)
        neighbourhood_df = (
            premises_monthly
            .select(
                col("PREMISES_TYPE").alias("type"),
                col("total_cases").alias("count"),
                col("OCC_YEAR").alias("year"),
                col("OCC_MONTH").alias("month")
            )
        )

        return neighbourhood_df

    def close_spark_session(self):
        self.spark.stop()

