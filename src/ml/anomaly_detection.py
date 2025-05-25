import pandas as pd
import numpy as np
from typing import Dict, Any, Optional, Tuple
from darts import TimeSeries
from darts.models import Prophet
from darts.dataprocessing.transformers import Scaler
import logging

from .utils import prepare_time_series, detect_anomalies

logger = logging.getLogger(__name__)

class AnomalyDetector:
    """
    A class for detecting anomalies in time series data using Prophet.
    """
    
    def __init__(self, model_params: Optional[Dict[str, Any]] = None):
        """
        Initialize the anomaly detector.
        
        Args:
            model_params: Parameters for the Prophet model
        """
        self.model = None
        self.scaler = Scaler()
        self.model_params = model_params or {}
        self.train_series = None
        self.test_series = None
    
    def fit(self, df: pd.DataFrame, time_col: str = 'timestamp', 
            value_col: str = 'consumption_mwh', test_size: float = 0.2) -> 'AnomalyDetector':
        """
        Fit the anomaly detection model.
        
        Args:
            df: DataFrame containing the time series data
            time_col: Name of the timestamp column
            value_col: Name of the value column
            test_size: Fraction of data to use for testing
            
        Returns:
            self: Returns the instance itself
        """
        try:
            # Prepare time series data
            train_series, test_series = prepare_time_series(
                df, time_col, value_col, test_size=test_size
            )
            
            # Scale the data
            self.scaler.fit(train_series)
            scaled_train = self.scaler.transform(train_series)
            
            # Train Prophet model
            self.model = Prophet(**self.model_params)
            self.model.fit(scaled_train)
            
            # Store series for later use
            self.train_series = train_series
            self.test_series = test_series
            
            return self
            
        except Exception as e:
            logger.error(f"Error fitting anomaly detector: {str(e)}")
            raise
    
    def predict(self, df: pd.DataFrame, time_col: str = 'timestamp', 
                value_col: str = 'consumption_mwh', threshold_std: float = 2.0) -> pd.DataFrame:
        """
        Detect anomalies in the given data.
        
        Args:
            df: DataFrame containing the time series data
            time_col: Name of the timestamp column
            value_col: Name of the value column
            threshold_std: Number of standard deviations to use as threshold
            
        Returns:
            DataFrame with anomaly information
        """
        if self.model is None:
            raise ValueError("Model has not been fitted. Call fit() first.")
            
        try:
            # Create time series from input data
            series = TimeSeries.from_dataframe(
                df, time_col=time_col, value_cols=value_col
            )
            
            # Scale the data
            scaled_series = self.scaler.transform(series)
            
            # Make predictions
            predictions = self.model.predict(len(series))
            predictions = self.scaler.inverse_transform(predictions)
            
            # Detect anomalies
            results = detect_anomalies(series, predictions, threshold_std)
            
            return results
            
        except Exception as e:
            logger.error(f"Error predicting anomalies: {str(e)}")
            raise
    
    def evaluate(self) -> Dict[str, float]:
        """
        Evaluate the model on the test set.
        
        Returns:
            Dictionary of evaluation metrics
        """
        if self.model is None or self.train_series is None or self.test_series is None:
            raise ValueError("Model has not been fitted. Call fit() first.")
            
        try:
            # Scale the test series
            scaled_test = self.scaler.transform(self.test_series)
            
            # Make predictions
            predictions = self.model.predict(len(self.test_series))
            predictions = self.scaler.inverse_transform(predictions)
            
            # Calculate metrics
            from darts.metrics import mae, mse, rmse, mape
            
            metrics = {
                'mae': mae(self.test_series, predictions),
                'mse': mse(self.test_series, predictions),
                'rmse': rmse(self.test_series, predictions),
                'mape': mape(self.test_series, predictions)
            }
            
            return metrics
            
        except Exception as e:
            logger.error(f"Error evaluating model: {str(e)}")
            raise
    
    def save_model(self, filepath: str) -> None:
        """
        Save the trained model to a file.
        
        Args:
            filepath: Path to save the model
        """
        if self.model is None:
            raise ValueError("No model to save. Fit the model first.")
            
        try:
            import joblib
            joblib.dump({
                'model': self.model,
                'scaler': self.scaler,
                'model_params': self.model_params
            }, filepath)
            
        except Exception as e:
            logger.error(f"Error saving model: {str(e)}")
            raise
    
    @classmethod
    def load_model(cls, filepath: str) -> 'AnomalyDetector':
        """
        Load a trained model from a file.
        
        Args:
            filepath: Path to the saved model
            
        Returns:
            AnomalyDetector instance with loaded model
        """
        try:
            import joblib
            
            # Load the model and parameters
            data = joblib.load(filepath)
            
            # Create a new instance
            detector = cls(model_params=data.get('model_params', {}))
            detector.model = data['model']
            detector.scaler = data['scaler']
            
            return detector
            
        except Exception as e:
            logger.error(f"Error loading model: {str(e)}")
            raise
