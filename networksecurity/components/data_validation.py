import os
import sys

import pandas as pd
from scipy.stats import ks_2samp

from networksecurity.exception.exception import NetworkSecurityException
from networksecurity.logging.logger import logging
# configuration of the Data Validation Module
from networksecurity.entity.config_entity import DataValidationConfig
from networksecurity.entity.artifact_entity import DataIngestionArtifact, DataValidationArtifact
from networksecurity.constant.training_pipeline import SCHEMA_FILE_PATH
from networksecurity.utils.main_utils.utils import read_yaml_file, write_yaml_file


class DataValidation:
    def __init__(self, data_ingestion_artifact: DataIngestionArtifact, data_validation_config: DataValidationConfig):
        try:
            self.data_ingestion_artifact = data_ingestion_artifact
            self.data_validation_config = data_validation_config
            self.schema_config = read_yaml_file(SCHEMA_FILE_PATH)
        except Exception as e:
            raise NetworkSecurityException(e, sys)
    
    @staticmethod
    def read_data(file_path: str) -> pd.DataFrame:
        try:
            return pd.read_csv(file_path)
        except Exception as e:
            raise NetworkSecurityException(e, sys)
    
    def validate_number_of_columns(self, dataframe: pd.DataFrame) -> bool:
        try:
            number_of_columns = len(self.schema_config["columns"])
            logging.info(f"Required number of columns:{number_of_columns}")
            logging.info(f"Data frame has columns:{len(dataframe.columns)}")
            if len(dataframe.columns) == number_of_columns:
                return True
            else:
                return False
        except Exception as e:
            raise NetworkSecurityException(e, sys)
    
    def is_numerical_column(self, dataframe: pd.DataFrame) -> bool:
        try:
            numerical_columns = self.schema_config["numerical_columns"]
            dataframe_columns = dataframe.columns

            # Check if all numerical columns from schema exist in dataframe
            numerical_column_present = all(column in dataframe_columns 
                                        for column in numerical_columns)
            
            if not numerical_column_present:
                return False

            # Check if all these columns have numerical data
            for column in numerical_columns:
                if dataframe[column].dtype not in ['int64', 'float64']:
                    return False

            return True
            
        except Exception as e:
            raise NetworkSecurityException(e, sys)
        
    def detect_dataset_drift(self, base_df, current_df, threshold=0.05) -> bool:
        """
        Detect drift between two datasets using Kolmogorov-Smirnov test
        """
        try:
            status = True
            report = {}
            for column in base_df:
                d1 = base_df[column]
                d2 = current_df[column]
                is_same_dist = ks_2samp(d1, d2)
                if threshold <= is_same_dist.pvalue:
                    is_found = False
                else:
                    is_found = True
                    status = False
                report.update({column:{
                    "p_value":float(is_same_dist.pvalue),
                    "drift_status":is_found
                    }})
            drift_report_file_path = self.data_validation_config.drift_report_file_path
            # create the drift report directory
            os.makedirs(os.path.dirname(drift_report_file_path), exist_ok=True)
            write_yaml_file(drift_report_file_path, content=report, replace=True)
            return status
        except Exception as e:
            raise NetworkSecurityException(e, sys)
    
    def initiate_data_validation(self) -> DataValidationArtifact:
        try:
            train_file_path = self.data_ingestion_artifact.trained_file_path
            test_file_path = self.data_ingestion_artifact.test_file_path
            
            # read data from train and test files
            train_df = DataValidation.read_data(train_file_path)
            test_df = DataValidation.read_data(test_file_path)
            
            # validate number of columns
            status = self.validate_number_of_columns(train_df)
            if not status:
                error_message = "Train dataframe does not contain all columns. \n"
            status = self.validate_number_of_columns(test_df)
            if not status:
                error_message = "Test dataframe does not contain all columns. \n"
            
            # validate numerical columns
            status = self.is_numerical_column(train_df)
            if not status:
                error_message = f"Train dataframe contains non-numerical values in numerical columns.\n"
            status = self.is_numerical_column(test_df)
            if not status:
                error_message = f"Test dataframe contains non-numerical values in numerical columns.\n"
            
            # check for data drift
            status = self.detect_dataset_drift(base_df=train_df, current_df=test_df)
            
            # create the data validation artifact
            dir_path = os.path.dirname(self.data_validation_config.valid_train_file_path)
            os.makedirs(dir_path, exist_ok=True)
            train_df.to_csv(self.data_validation_config.valid_train_file_path, index=False, header=True)
            test_df.to_csv(self.data_validation_config.valid_test_file_path, index=False, header=True)
            
            data_validation_artifact = DataValidationArtifact(validation_status=status,
                                                              valid_train_file_path=self.data_validation_config.valid_train_file_path,
                                                              valid_test_file_path=self.data_validation_config.valid_test_file_path,
                                                              invalid_train_file_path=None,
                                                              invalid_test_file_path=None,
                                                              drift_report_file_path=self.data_validation_config.drift_report_file_path)
            return data_validation_artifact
        except Exception as e:
            raise NetworkSecurityException(e, sys)