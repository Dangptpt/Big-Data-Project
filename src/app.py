from data_transformation.init_transformation import DataTransformationInit
from data_importer.data_importer import DataImporter
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
        input_path = "../../spark/app/data/clean_data.csv"
        # input_path = "data/clean_data.csv"
        # input_path = "hdfs://namenode:9000/output/crime"
        crime_df = processor.load_and_prepare_data(input_path)

        temporal_df = processor.process_temporal_data(crime_df)
        division_df = processor.process_division_data(crime_df)
        neighbourhood_df = processor.process_neighbourhood_data(crime_df)
        temporal_df.show()
        division_df.show()
        neighbourhood_df.show()
        logging.info("Data transformation complete")
        logging.info("Starting data import")

        importer.import_data(temporal_df, "temporal_data")
        importer.import_data(division_df, "division_data")
        importer.import_data(neighbourhood_df, "neighbourhood_data")
        logging.info("Data import complete")
    except Exception as e:
        logging.error(f"An error occurred: {e}")
    
    finally:
        logging.info("Stopping Spark session")
        processor.spark.stop()


if __name__ == "__main__":
    main()