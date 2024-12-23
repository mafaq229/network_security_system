import sys

from networksecurity.components.data_ingestion import DataIngestion
from networksecurity.entity.config_entity import DataIngestionConfig, TrainingPipelineConfig
from networksecurity.components.data_validation import DataValidation
from networksecurity.entity.config_entity import DataValidationConfig
from networksecurity.components.data_transformation import DataTransformation
from networksecurity.entity.config_entity import DataTransformationConfig

from networksecurity.exception.exception import NetworkSecurityException
from networksecurity.logging.logger import logging

if __name__ == "__main__":
    try:
        logging.info("Initiate the data ingestion")
        data_ingestion = DataIngestion(DataIngestionConfig(TrainingPipelineConfig()))
        data_ingestion_artifact = data_ingestion.initiate_data_ingestion()
        logging.info("Data Ingestion Completed")
        logging.info("Initiate the data validation")
        data_valdation = DataValidation(data_ingestion_artifact=data_ingestion_artifact, 
                                        data_validation_config=DataValidationConfig(TrainingPipelineConfig()))
        data_validation_artifact = data_valdation.initiate_data_validation()
        logging.info("Data Validation Completed")
        logging.info("Initiate the data transformation")
        data_transformation = DataTransformation(data_validation_artifact=data_validation_artifact, 
                                                 data_transformation_config=DataTransformationConfig(TrainingPipelineConfig()))
        data_transformation_artifact = data_transformation.initiate_data_transformation()
        logging.info("Data Transformation Completed")
        
        print(data_transformation_artifact)
    except Exception as e:
            raise NetworkSecurityException(e, sys)