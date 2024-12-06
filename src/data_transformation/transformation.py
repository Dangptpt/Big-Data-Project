from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from pyspark.sql.types import *
from pyspark.ml.regression import RandomForestRegressor
from pyspark.ml.feature import VectorAssembler, MinMaxScaler, StandardScaler
from pyspark.ml.clustering import KMeans
from pyspark.ml import Pipeline

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

    def process_security_data(self, df):
        # 0. Clean data - replace nulls with 0
        df = df.withColumn("BIKE_COST", coalesce(col("BIKE_COST"), lit(0)))

        # 1. Initial aggregation
        ml_df = df.groupBy(
            "OCC_YEAR",
            "OCC_MONTH",
            "DIVISION"
        ).agg(
            count("BIKE_COST").alias("case_count"),
            sum("BIKE_COST").alias("total_cost"),
            avg("BIKE_COST").alias("avg_cost")
        ).orderBy(
            "OCC_YEAR",
            "OCC_MONTH"
        )
             
        # 2. Add normalized columns at the end
        window = Window.partitionBy()
        final_df = ml_df

        for col_name in ["case_count", "total_cost"]:
            min_val = min(col(col_name)).over(window)
            max_val = max(col(col_name)).over(window)
            norm_col = f"normalized_{col_name}"
            
            final_df = final_df.withColumn(
                norm_col, 
                (col(col_name) - min_val) / (max_val - min_val)
            )
        final_df = final_df.withColumn("coefficient", col("normalized_case_count") + col("normalized_total_cost"))
        
        return final_df
    
    def create_monthly_division_summary(self, df):
        
        result_df = df.groupBy(
            "OCC_MONTH",
            "DIVISION"
        ).agg(
            count("*").alias("total_cases"),
        )

        return result_df

    def security_risk_clustering(self, df):
        # Initial aggregation
        df = df.withColumn("BIKE_COST", coalesce(col("BIKE_COST"), lit(0)))
        
        spark_df = df.groupBy("DIVISION").agg(
            count("BIKE_COST").alias("case_count"),
            sum("BIKE_COST").alias("total_cost")
        )

        # Prepare features for clustering
        assembler = VectorAssembler(
            inputCols=["case_count", "total_cost"],
            outputCol="features"
        )
        
        df_features = assembler.transform(spark_df)

        # Standardize features
        scaler = StandardScaler(
            inputCol="features",
            outputCol="scaled_features",
            withStd=True,
            withMean=True
        )
        
        scaler_model = scaler.fit(df_features)
        df_scaled = scaler_model.transform(df_features)

        # Apply KMeans clustering
        kmeans = KMeans(
            k=4,
            featuresCol="scaled_features",
            predictionCol="cluster"
        )
        
        model = kmeans.fit(df_scaled)
        df_clustered = model.transform(df_scaled)

        # Calculate risk scores based on cluster centers magnitude
        centers = model.clusterCenters()
        # Calculate magnitude of each cluster center
        magnitudes = [float(np.linalg.norm(center)) for center in centers]
        # Create mapping from cluster index to risk level (0-5)
        risk_mapping = dict(enumerate(np.argsort(magnitudes)))

        # Create UDF for mapping clusters to risk levels
        @udf(IntegerType())
        def get_risk_level(cluster_id):
            return int(risk_mapping[cluster_id])

        # Apply mapping and select final columns
        result_df = df_clustered.select(
            "DIVISION",
            get_risk_level(col("cluster")).alias("SECURITY_LEVEL"),
            col("case_count").alias("CASE_COUNT"),
            col("total_cost").alias("TOTAL_COST")
        ).orderBy("DIVISION")

        return result_df

    # def security_risk_clustering(self, df):
    #     # Initial aggregation remains same
    #     df = df.withColumn("BIKE_COST", coalesce(col("BIKE_COST"), lit(0)))
        
    #     spark_df = df.groupBy("DIVISION").agg(
    #         count("BIKE_COST").alias("case_count"),
    #         sum("BIKE_COST").alias("total_cost")
    #     )

    #     # Get min and max values for normalization
    #     stats = spark_df.agg(
    #         min("case_count").alias("min_cases"),
    #         max("case_count").alias("max_cases"),
    #         min("total_cost").alias("min_cost"),
    #         max("total_cost").alias("max_cost")
    #     ).collect()[0]

    #     # Add normalized columns using min-max scaling
    #     normalized_df = spark_df.withColumn(
    #         "normalized_case_count",
    #         (col("case_count") - stats.min_cases) / (stats.max_cases - stats.min_cases)
    #     ).withColumn(
    #         "normalized_total_cost",
    #         (col("total_cost") - stats.min_cost) / (stats.max_cost - stats.min_cost)
    #     )

    #     # Prepare features for clustering
    #     assembler = VectorAssembler(
    #         inputCols=["normalized_case_count", "normalized_total_cost"],
    #         outputCol="features"
    #     )
        
    #     df_features = assembler.transform(normalized_df)

    #     # KMeans clustering
    #     kmeans = KMeans(k=4, featuresCol="features", predictionCol="cluster")
    #     model = kmeans.fit(df_features)
    #     df_clustered = model.transform(df_features)

    #     # Risk mapping remains same
    #     centers = model.clusterCenters()
    #     magnitudes = [float(np.linalg.norm(center)) for center in centers]
    #     risk_mapping = dict(enumerate(np.argsort(magnitudes)))

    #     @udf(IntegerType())
    #     def get_risk_level(cluster_id):
    #         return int(risk_mapping[cluster_id])

    #     # Final result with normalized values
    #     result_df = df_clustered.select(
    #         "DIVISION",
    #         get_risk_level(col("cluster")).alias("CLUSTERING"),
    #         col("normalized_case_count").alias("NORMALIZED_CASE_COUNT"),
    #         col("normalized_total_cost").alias("NORMALIZED_TOTAL_COST")
    #     ).orderBy("DIVISION")

    #     return result_df