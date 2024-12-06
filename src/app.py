from data_transformation.transformation import DataTransformationInit
from data_loader.loader import DataImporter
import logging
from config import Config

config = Config()

def main():
    logging.basicConfig(level=logging.INFO)
    logging.info("Starting data transformation")
    # HDFS Service
    ##

    processor = DataTransformationInit()
    importer = DataImporter(
        host=config.HOST,
        port=config.PORT,
        dbname=config.DATABASE,
        user=config.USER,
        password=config.PASSWORD
    )

    try:
        # input_path = "../../spark/app/data/clean_data.csv"
        # input_path = "data/clean_data.csv"
        # load all csv files path in hdfs
        input_path = "hdfs://hdfs-namenode:9000/data"
        
        crime_df = processor.load_and_prepare_data(input_path)

        premise_df = processor.process_premise_data(crime_df)  
        temporal_df = processor.process_temporal_data(crime_df)
        division_df = processor.process_division_data(crime_df)
        neighbourhood_df = processor.process_neighbourhood_data(crime_df)
        
        security_risk_df = processor.security_risk_clustering(crime_df)
        month_division_df = processor.create_monthly_division_summary(crime_df)

        crime_df.show()
        premise_df.show()
        temporal_df.show()
        division_df.show()
        neighbourhood_df.show()

        security_risk_df.show()
        month_division_df.show()

        logging.info("Data transformation complete")
        logging.info("Starting data import")

        importer.import_data(crime_df, "crime")
        importer.import_data(premise_df, "premise")
        importer.import_data(temporal_df, "temporal")
        importer.import_data(division_df, "division")
        importer.import_data(neighbourhood_df, "neighbourhood")

        importer.import_data(security_risk_df, "security_clustering")
        importer.import_data(month_division_df, "monthly_division")


        logging.info("Data import complete")
    except Exception as e:
        logging.error(f"An error occurred: {e}")
    
    finally:
        logging.info("Stopping Spark session")
        processor.spark.stop()


if __name__ == "__main__":
    main()
