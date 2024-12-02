from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, col
import os
import sys

spark = SparkSession.builder \
    .appName("PostgrsSQL demo") \
    .getOrCreate()

def load_dataframe(df, table_name):
    df.write \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://aws-0-ap-southeast-1.pooler.supabase.com:5432/postgres?user=postgres.rikgpxxmufvsupalsnkx&password=bigdata2024.1") \
        .option("dbtable", table_name) \
        .option("user", "postgres.rikgpxxmufvsupalsnkx") \
        .option("password", "bigdata2024.1") \
        .option("driver", "org.postgresql.Driver") \
        .mode("overwrite") \
        .save()
    

data_spark = spark.read.csv('data/clean_data.csv', header=True, inferSchema=True)

# Group by PREMISES_TYPE, OCC_YEAR, and OCC_MONTH to calculate total cases
premises_monthly = (
    data_spark.groupBy("PREMISES_TYPE", "OCC_YEAR", "OCC_MONTH")
    .count()
    .withColumnRenamed("count", "total_cases")
)

# Calculate average total cases for each PREMISES_TYPE across all years
average_yearly = (
    premises_monthly.groupBy("PREMISES_TYPE")
    .agg(avg("total_cases").alias("average_cases"))
)

# Join monthly data with average yearly data to compute % change (only based on year)
df = (
    premises_monthly
    .select(
        col("PREMISES_TYPE").alias("type"),
        col("total_cases").alias("count"),
        col("OCC_YEAR").alias("year"),
        col("OCC_MONTH").alias("month")
    )
)

# Group by DIVISION, PREMISES_TYPE, and OCC_YEAR to calculate total cases
division_yearly = (
    data_spark.groupBy("DIVISION", "PREMISES_TYPE", "OCC_YEAR")
    .count()
    .withColumnRenamed("count", "total_cases")
)

# Calculate average total cases for each DIVISION and PREMISES_TYPE across all years
average_by_division = (
    division_yearly.groupBy("DIVISION", "PREMISES_TYPE")
    .agg(avg("total_cases").alias("average_cases"))
)

# Join yearly data with average data to compute % change
division_result = (
    division_yearly.join(average_by_division, on=["DIVISION", "PREMISES_TYPE"])
    .withColumn("change", (col("total_cases") - col("average_cases")) / col("average_cases") * 100)
    .select(
        col("DIVISION").alias("division"),
        col("PREMISES_TYPE").alias("premise_type"),
        col("total_cases").alias("count"),
        col("OCC_YEAR").alias("year")
    )
)

temporal_hour = (
    data_spark.groupBy("OCC_HOUR", "OCC_YEAR")
    .count()
    .withColumnRenamed("count", "total_cases")
    .select(
        col("total_cases").alias("count"),
        col("OCC_HOUR").alias("hour"),
        col("OCC_YEAR").alias("year")
    )
)

# TEMPORAL (day_of_week) Table: Group by OCC_DOW and OCC_YEAR
temporal_day_of_week = (
    data_spark.groupBy("OCC_DOW", "OCC_YEAR")
    .count()
    .withColumnRenamed("count", "total_cases")
    .select(
        col("total_cases").alias("count"),
        col("OCC_DOW").alias("day_of_week"),
        col("OCC_YEAR").alias("year")
    )
)

neighborhood_yearly = (
    data_spark.groupBy("NEIGHBOURHOOD_158", "PREMISES_TYPE", "OCC_YEAR")
    .count()
    .withColumnRenamed("count", "total_cases")
)

# Calculate average total cases for each NEIGHBORHOOD and PREMISES_TYPE across all years
average_by_neighborhood = (
    neighborhood_yearly.groupBy("NEIGHBOURHOOD_158", "PREMISES_TYPE")
    .agg(avg("total_cases").alias("average_cases"))
)

# Join yearly data with average data to compute % change
neighborhood_result = (
    neighborhood_yearly.join(average_by_neighborhood, on=["NEIGHBOURHOOD_158", "PREMISES_TYPE"])
    .withColumn("change", (col("total_cases") - col("average_cases")) / col("average_cases") * 100)
    .select(
        col("NEIGHBOURHOOD_158").alias("neighborhood"),
        col("total_cases").alias("count"),
        col("PREMISES_TYPE").alias("premise_type"),
        col("OCC_YEAR").alias("year")
    )
)

load_dataframe(df, "premise")
load_dataframe(division_result, "division")
load_dataframe(temporal_hour, "temporal_hour")
load_dataframe(temporal_day_of_week, "temporal_day_of_week")
load_dataframe(neighborhood_result, "neighborhood")

temporal = (
    data_spark.groupBy("OCC_HOUR", "OCC_DOW", "OCC_YEAR")
    .count()
    .withColumnRenamed("count", "total_cases")
    .select(
        col("total_cases").alias("count"),
        col("OCC_HOUR").alias("hour"),
        col("OCC_DOW").alias("day_of_week"),
        col("OCC_YEAR").alias("year")
    )
)


load_dataframe(temporal, "temporal")