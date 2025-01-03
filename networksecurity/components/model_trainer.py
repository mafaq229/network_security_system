import os
import sys

from sklearn.ensemble import RandomForestClassifier, GradientBoostingClassifier, AdaBoostClassifier
from sklearn.tree import DecisionTreeClassifier
from sklearn.linear_model import LogisticRegression
import mlflow
import dagshub

from networksecurity.exception.exception import NetworkSecurityException
from networksecurity.logging.logger import logging
from networksecurity.constant.training_pipeline import SAVED_MODEL_DIR,MODEL_FILE_NAME
from networksecurity.entity.artifact_entity import ModelTrainerArtifact, DataTransformationArtifact, ClassificationMetricArtifact
from networksecurity.entity.config_entity import ModelTrainerConfig
from networksecurity.utils.ml_utils.model.estimator import NetworkModel
from networksecurity.utils.ml_utils.metric.classification_metric import get_classification_score
from networksecurity.utils.main_utils.utils import save_object, load_object, load_numpy_array_data, evaluate_models


# now mlflows knows where to log experiments
dagshub.init(repo_owner='muhammadafaq1999', repo_name='network_security_system', mlflow=True)


class ModelTrainer:
    def __init__(self, data_transformation_artifact: DataTransformationArtifact, 
                 model_trainer_config: ModelTrainerConfig):
        try:
            self.model_trainer_config = model_trainer_config
            self.data_transformation_artifact = data_transformation_artifact
        except Exception as e:
            raise NetworkSecurityException(e,sys)
    
    def track_mlflow(self, best_model, classification_metric: ClassificationMetricArtifact):
        with mlflow.start_run():
            f1_score = classification_metric.f1_score
            precision_score = classification_metric.precision_score
            recall_score = classification_metric.recall_score
            
            mlflow.log_metric("f1_score", f1_score)
            mlflow.log_metric("precision_score", precision_score)
            mlflow.log_metric("recall_score", recall_score)
            
            mlflow.sklearn.log_model(best_model, "model")
            
    
    def train_model(self, X_train, Y_train, X_test, Y_test):
        models = {
                "Random Forest": RandomForestClassifier(verbose=1),
                "Decision Tree": DecisionTreeClassifier(),
                "Gradient Boosting": GradientBoostingClassifier(verbose=1),
                "Logistic Regression": LogisticRegression(verbose=1),
                "AdaBoost": AdaBoostClassifier(),
            }
        params={
            "Decision Tree": {
                'criterion':['gini', 'entropy', 'log_loss'],
                # 'splitter':['best','random'],
                # 'max_features':['sqrt','log2'],
            },
            "Random Forest":{
                # 'criterion':['gini', 'entropy', 'log_loss'],
                
                # 'max_features':['sqrt','log2',None],
                'n_estimators': [8,16,32,128,256]
            },
            "Gradient Boosting":{
                # 'loss':['log_loss', 'exponential'],
                'learning_rate':[.1,.01,.05,.001],
                'subsample':[0.6,0.7,0.75,0.85,0.9],
                # 'criterion':['squared_error', 'friedman_mse'],
                # 'max_features':['auto','sqrt','log2'],
                'n_estimators': [8,16,32,64,128,256]
            },
            "Logistic Regression":{},
            "AdaBoost":{
                'learning_rate':[.1,.01,.001],
                'n_estimators': [8,16,32,64,128,256]
            }
        }
        
        model_report: dict = evaluate_models(X_train=X_train, Y_train=Y_train, 
                                             X_test=X_test, Y_test=Y_test, models=models, params=params)
        # get best model score from dict
        best_model_score = max(sorted(model_report.values()))
        # get best model name from dict
        best_model_name = list(model_report.keys())[list(model_report.values()).index(best_model_score)]
        best_model = models[best_model_name]
        
        Y_train_pred = best_model.predict(X_train)
        classification_train_metric = get_classification_score(Y_train, Y_train_pred)
        Y_test_pred=best_model.predict(X_test)
        classification_test_metric=get_classification_score(Y_test, Y_test_pred)
        
        # Track experiments with the mlflow
        self.track_mlflow(best_model, classification_train_metric)
        self.track_mlflow(best_model, classification_test_metric)
        
        preprocessor = load_object(file_path=self.data_transformation_artifact.transformed_object_file_path)
        
        model_dir_path = os.path.dirname(self.model_trainer_config.trained_model_file_path)
        os.makedirs(model_dir_path, exist_ok=True)
        
        network_model = NetworkModel(preprocessor, best_model)
        save_object(self.model_trainer_config.trained_model_file_path, obj=network_model)
        save_object("final_model/model.pkl",best_model)
        
        # model trainer artifact
        model_trainer_artifact = ModelTrainerArtifact(trained_model_file_path=self.model_trainer_config.trained_model_file_path,
                                                      train_metric_artifact=classification_train_metric,
                                                      test_metric_artifact=classification_test_metric)
        logging.info(f"Model trainer artifact: {model_trainer_artifact}")
        return model_trainer_artifact       
    
    def initiate_model_trainer(self) -> ModelTrainerArtifact:
        try:
            train_file_path = self.data_transformation_artifact.transformed_train_file_path
            test_file_path = self.data_transformation_artifact.transformed_test_file_path
            
            # loading train and test arrays
            train_arr = load_numpy_array_data(train_file_path)
            test_arr = load_numpy_array_data(test_file_path)
            x_train, y_train, x_test, y_test = (
                train_arr[:, :-1],
                train_arr[:, -1],
                test_arr[:, :-1],
                test_arr[:, -1],
            )
            
            
            model_trainer_artifact = self.train_model(x_train, y_train, x_test, y_test)
            return model_trainer_artifact            
        except Exception as e:
            raise NetworkSecurityException(e,sys)

