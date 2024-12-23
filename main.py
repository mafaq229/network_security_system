import sys

from networksecurity.components.data_ingestion import DataIngestion
from networksecurity.entity.config_entity import DataIngestionConfig, TrainingPipelineConfig

from networksecurity.exception.exception import NetworkSecurityException
from networksecurity.logging.logger import logging

if __name__ == "__main__":
    try:
        data_ingestion = DataIngestion(DataIngestionConfig(TrainingPipelineConfig()))
        logging.info("Initiate the data ingestion")
        data_ingestion_artifact = data_ingestion.initiate_data_ingestion()
        logging.info("Data Ingestion Completed")
        print(data_ingestion_artifact)
    except Exception as e:
            raise NetworkSecurityException(e, sys)