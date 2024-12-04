from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from pyspark.sql.types import *
from pyspark.sql.functions import col, when, to_date, datediff, upper, trim, coalesce
from pyspark.sql.types import DoubleType

class DataTransformationInit:
    def __init__(self) -> None:
        self.spark = SparkSession.builder \
            .appName("CrimeAnalytics") \
            .getOrCreate()
    
    from pyspark.sql import DataFrame

    def clean_bicycle_data(self, df):
        df_clean = df.withColumn(
            'PREMISES_TYPE',
            when(col('PREMISES_TYPE').isin(['Apartment', 'House']), 'Residential')
            .when(col('PREMISES_TYPE') == 'Transit', 'Other')
            .when(col('PREMISES_TYPE') == 'Educational', 'Commercial')
            .otherwise(col('PREMISES_TYPE'))
        )
        
        df_clean = df_clean.withColumn('OCC_DATE', to_date(col('OCC_DATE'))) \
                        .withColumn('REPORT_DATE', to_date(col('REPORT_DATE')))
        
        df_clean = df_clean.withColumn(
            'BIKE_COST',
            when(col('BIKE_COST').isNotNull(), col('BIKE_COST').cast(DoubleType()))
            .otherwise(lit(0.0))
        )
        
        if 'BIKE_SPEED' in df.columns:
            df_clean = df_clean.withColumn(
                'BIKE_SPEED',
                when(col('BIKE_SPEED').rlike('^[0-9]+(\\.[0-9]+)?$'), col('BIKE_SPEED').cast(DoubleType()))
                .otherwise(lit(None))
            )
            
            median_speed = df_clean.filter(col('BIKE_SPEED').isNotNull()) \
                                .approxQuantile('BIKE_SPEED', [0.5], 0.01)[0]
            
            df_clean = df_clean.withColumn(
                'BIKE_SPEED',
                when(col('BIKE_SPEED').isNull(), lit(median_speed))
                .otherwise(col('BIKE_SPEED'))
            )
        
        df_clean = df_clean.withColumn('BIKE_TYPE', coalesce(col('BIKE_TYPE'), lit('UNKNOWN'))) \
                        .withColumn('LOCATION_TYPE', coalesce(col('LOCATION_TYPE'), lit('UNKNOWN'))) \
                        .withColumn('PREMISES_TYPE', coalesce(col('PREMISES_TYPE'), lit('Other')))
        
        df_clean = df_clean.withColumn(
            'REPORT_DELAY',
            when(col('OCC_DATE').isNotNull() & col('REPORT_DATE').isNotNull(),
                datediff(col('REPORT_DATE'), col('OCC_DATE')) * 24.0)
            .otherwise(lit(None))
        )
        
        df_clean = df_clean.withColumn('BIKE_MAKE', upper(trim(col('BIKE_MAKE')))) \
                        .withColumn('BIKE_MODEL', upper(trim(col('BIKE_MODEL'))))
        
        return df_clean

        
    def load_and_prepare_data(self, input_path):
        df = self.spark.read.csv(input_path, header=True, inferSchema=True)
        df = self.clean_bicycle_data(df)
        date_columns = ['OCC_DATE', 'REPORT_DATE']
        for column in date_columns:
            df = df.withColumn(column, to_timestamp(column, 'M/d/yyyy h:mm:ss a'))

        df = df.withColumn("BIKE_COST", col("BIKE_COST").cast("double")) \
            .withColumn("BIKE_SPEED", col("BIKE_SPEED").cast("integer")) \
            .withColumn("LAT_WGS84", col("LAT_WGS84").cast("double")) \
            .withColumn("LONG_WGS84", col("LONG_WGS84").cast("double"))

        return df
    
    def process_premise_data(self, df):
        premises_monthly = (
            df.groupBy("PREMISES_TYPE", "OCC_YEAR", "OCC_MONTH")
            .agg(
                count("*").alias("total_cases"),
                sum(when(col("BIKE_COST") != 0, col("BIKE_COST")).otherwise(0)).alias("sum_non_zero"),
                sum(when(col("BIKE_COST") != 0, 1).otherwise(0)).alias("count_non_zero")
            )
            .withColumn(
                "avg_value",
                (col("sum_non_zero") / col("count_non_zero")).alias("avg_value")
            )
        )

        premise_df = (
            premises_monthly.select(
                col("PREMISES_TYPE").alias("type"),
                col("total_cases").alias("total_cases"),
                col("avg_value").alias("avg_value"),
                col("OCC_YEAR").alias("year"),
                col("OCC_MONTH").alias("month")
            )
        )

        return premise_df
        
    def process_temporal_data(self, df):
        temporal_df = (
            df.groupBy("OCC_HOUR", "OCC_DOW", "OCC_YEAR")
            .agg(
                count("*").alias("total_cases"), 
                sum(when(col("BIKE_COST") != 0, col("BIKE_COST")).otherwise(0)).alias("total_value"),
                sum(when(col("BIKE_COST") != 0, 1).otherwise(0)).alias("count_non_zero")
            )
            .withColumn(
                "avg_value",
                (col("total_value") / col("count_non_zero")).alias("avg_value")
            )
            .select(
                col("total_cases").alias("total_cases"),
                col("OCC_HOUR").alias("hour"),
                col("OCC_DOW").alias("day_of_week"),
                col("avg_value").alias("avg_value"),
                col("OCC_YEAR").alias("year")
            )
        )
        return temporal_df
    
    def process_division_data(self, df):
        division_yearly = (
            df.groupBy("DIVISION", "PREMISES_TYPE", "OCC_YEAR")
            .agg(
                count("*").alias("total_cases"), 
                sum(when(col("BIKE_COST") != 0, col("BIKE_COST")).otherwise(0)).alias("total_value"),
                sum(when(col("BIKE_COST") != 0, 1).otherwise(0)).alias("count_non_zero")
            )
            .withColumn(
                "avg_value",
                (col("total_value") / col("count_non_zero")).alias("avg_value")
            )
        )

        average_by_division = (
            division_yearly.groupBy("DIVISION", "PREMISES_TYPE")
            .agg(avg("total_cases").alias("average_cases"))
        )

        division_df = (
            division_yearly.join(average_by_division, on=["DIVISION", "PREMISES_TYPE"])
            .withColumn("change", (col("total_cases") - col("average_cases")) / col("average_cases") * 100)
            .select(
                col("DIVISION").alias("division"),
                col("PREMISES_TYPE").alias("premise_type"),
                col("total_cases").alias("total_cases"),
                col("OCC_YEAR").alias("year"),
                col("avg_value").alias("avg_value")
            )
        )

        return division_df
    
    def process_neighbourhood_data(self, df):
        premises_monthly = (
            df.groupBy("PREMISES_TYPE", "OCC_YEAR", "OCC_MONTH")
            .agg(
                count("*").alias("total_cases"), 
                sum(when(col("BIKE_COST") != 0, col("BIKE_COST")).otherwise(0)).alias("total_value"),
                sum(when(col("BIKE_COST") != 0, 1).otherwise(0)).alias("count_non_zero")
            )
            .withColumn(
                "avg_value",
                (col("total_value") / col("count_non_zero")).alias("avg_value")
                )
        )
        
        neighbourhood_df = (
            premises_monthly
            .select(
                col("PREMISES_TYPE").alias("type"),
                col("total_cases").alias("total_cases"),
                col("OCC_YEAR").alias("year"),
                col("avg_value").alias("avg_value"),
                col("OCC_MONTH").alias("month")
            )
        )

        return neighbourhood_df

    def close_spark_session(self):
        self.spark.stop()

