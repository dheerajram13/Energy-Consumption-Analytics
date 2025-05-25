from typing import Dict, Any, Optional, List, Tuple
import pandas as pd
import numpy as np
from darts import TimeSeries
from darts.models import Prophet, ExponentialSmoothing, ARIMA
from darts.metrics import mae, mse, rmse, mape
from darts.dataprocessing.transformers import Scaler
import logging

logger = logging.getLogger(__name__)

class EnergyForecaster:
    """
    A class for forecasting energy consumption using multiple models.
    """
    
    def __init__(self, model_type: str = 'prophet', model_params: Optional[Dict[str, Any]] = None):
        """
        Initialize the forecaster.
        
        Args:
            model_type: Type of model to use ('prophet', 'exponential_smoothing', 'arima')
            model_params: Parameters for the model
        """
        self.model_type = model_type.lower()
        self.model_params = model_params or {}
        self.model = None
        self.scaler = Scaler()
        self.train_series = None
        self.test_series = None
    
    def _get_model(self):
        """Get the appropriate model based on model_type."""
        if self.model_type == 'prophet':
            return Prophet(**self.model_params)
        elif self.model_type == 'exponential_smoothing':
            return ExponentialSmoothing(**self.model_params)
        elif self.model_type == 'arima':
            return ARIMA(**self.model_params)
        else:
            raise ValueError(f"Unsupported model type: {self.model_type}")
    
    def fit(self, df: pd.DataFrame, time_col: str = 'timestamp', 
            value_col: str = 'consumption_mwh', test_size: float = 0.2) -> 'EnergyForecaster':
        """
        Fit the forecasting model.
        
        Args:
            df: DataFrame containing the time series data
            time_col: Name of the timestamp column
            value_col: Name of the value column
            test_size: Fraction of data to use for testing
            
        Returns:
            self: Returns the instance itself
        """
        try:
            # Create time series
            series = TimeSeries.from_dataframe(
                df, time_col=time_col, value_cols=value_col
            )
            
            # Split into train/test
            train_size = int(len(series) * (1 - test_size))
            self.train_series = series[:train_size]
            self.test_series = series[train_size:]
            
            # Scale the data
            scaled_train = self.scaler.fit_transform(self.train_series)
            
            # Initialize and train the model
            self.model = self._get_model()
            self.model.fit(scaled_train)
            
            return self
            
        except Exception as e:
            logger.error(f"Error fitting forecasting model: {str(e)}")
            raise
    
    def predict(self, n_periods: int = 24) -> TimeSeries:
        """
        Make forecasts.
        
        Args:
            n_periods: Number of periods to forecast
            
        Returns:
            TimeSeries with the forecast
        """
        if self.model is None:
            raise ValueError("Model has not been fitted. Call fit() first.")
            
        try:
            # Make predictions
            forecast = self.model.predict(n_periods)
            
            # Inverse transform the predictions
            forecast = self.scaler.inverse_transform(forecast)
            
            return forecast
            
        except Exception as e:
            logger.error(f"Error making predictions: {str(e)}")
            raise
    
    def evaluate(self) -> Dict[str, float]:
        """
        Evaluate the model on the test set.
        
        Returns:
            Dictionary of evaluation metrics
        """
        if self.model is None or self.test_series is None:
            raise ValueError("Model has not been fitted. Call fit() first.")
            
        try:
            # Make predictions on test set
            predictions = self.model.predict(len(self.test_series))
            
            # Inverse transform the predictions and actual values
            predictions = self.scaler.inverse_transform(predictions)
            actual = self.scaler.inverse_transform(self.test_series)
            
            # Calculate metrics
            metrics = {
                'mae': mae(actual, predictions),
                'mse': mse(actual, predictions),
                'rmse': rmse(actual, predictions),
                'mape': mape(actual, predictions)
            }
            
            return metrics
            
        except Exception as e:
            logger.error(f"Error evaluating model: {str(e)}")
            raise
    
    def cross_validate(self, df: pd.DataFrame, time_col: str = 'timestamp', 
                      value_col: str = 'consumption_mwh', n_splits: int = 5) -> Dict[str, List[float]]:
        """
        Perform time series cross-validation.
        
        Args:
            df: DataFrame containing the time series data
            time_col: Name of the timestamp column
            value_col: Name of the value column
            n_splits: Number of splits for cross-validation
            
        Returns:
            Dictionary of evaluation metrics for each fold
        """
        from darts.utils.model_selection import train_test_split
        
        try:
            # Create time series
            series = TimeSeries.from_dataframe(
                df, time_col=time_col, value_cols=value_col
            )
            
            # Initialize metrics storage
            metrics = {
                'mae': [],
                'mse': [],
                'rmse': [],
                'mape': []
            }
            
            # Perform time series cross-validation
            for i in range(n_splits):
                # Split data
                train_size = int(len(series) * (i + 1) / (n_splits + 1))
                train, test = series[:train_size], series[train_size:]
                
                # Scale the data
                scaler = Scaler()
                scaled_train = scaler.fit_transform(train)
                
                # Train model
                model = self._get_model()
                model.fit(scaled_train)
                
                # Make predictions
                predictions = model.predict(len(test))
                predictions = scaler.inverse_transform(predictions)
                actual = scaler.inverse_transform(test)
                
                # Calculate metrics
                metrics['mae'].append(mae(actual, predictions))
                metrics['mse'].append(mse(actual, predictions))
                metrics['rmse'].append(rmse(actual, predictions))
                metrics['mape'].append(mape(actual, predictions))
            
            return metrics
            
        except Exception as e:
            logger.error(f"Error in cross-validation: {str(e)}")
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
                'model_type': self.model_type,
                'model_params': self.model_params
            }, filepath)
            
        except Exception as e:
            logger.error(f"Error saving model: {str(e)}")
            raise
    
    @classmethod
    def load_model(cls, filepath: str) -> 'EnergyForecaster':
        """
        Load a trained model from a file.
        
        Args:
            filepath: Path to the saved model
            
        Returns:
            EnergyForecaster instance with loaded model
        """
        try:
            import joblib
            
            # Load the model and parameters
            data = joblib.load(filepath)
            
            # Create a new instance
            forecaster = cls(
                model_type=data.get('model_type', 'prophet'),
                model_params=data.get('model_params', {})
            )
            forecaster.model = data['model']
            forecaster.scaler = data['scaler']
            
            return forecaster
            
        except Exception as e:
            logger.error(f"Error loading model: {str(e)}")
            raise
