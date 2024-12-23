import sys

from networksecurity.components.data_ingestion import DataIngestion
from networksecurity.entity.config_entity import DataIngestionConfig, TrainingPipelineConfig
from networksecurity.components.data_validation import DataValidation
from networksecurity.entity.config_entity import DataValidationConfig

from networksecurity.exception.exception import NetworkSecurityException
from networksecurity.logging.logger import logging

if __name__ == "__main__":
    try:
        data_ingestion = DataIngestion(DataIngestionConfig(TrainingPipelineConfig()))
        logging.info("Initiate the data ingestion")
        data_ingestion_artifact = data_ingestion.initiate_data_ingestion()
        logging.info("Data Ingestion Completed")
        logging.info("Initiate the data validation")
        data_valdation = DataValidation(data_validation_config=DataValidationConfig(TrainingPipelineConfig()),
                                        data_ingestion_artifact=data_ingestion_artifact)
        logging.info("Initiate the data validation")
        data_validation_artifact = data_valdation.initiate_data_validation()
        logging.info("Data Validation Completed")
        print(data_validation_artifact)
    except Exception as e:
            raise NetworkSecurityException(e, sys)