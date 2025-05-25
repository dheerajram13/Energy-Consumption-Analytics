import pandas as pd
import numpy as np
from typing import List, Tuple, Optional
from darts import TimeSeries
from darts.models import Prophet
from darts.dataprocessing.transformers import Scaler
from darts.metrics import mae, mse, rmse, mape
import logging

logger = logging.getLogger(__name__)

def prepare_time_series(
    df: pd.DataFrame,
    time_col: str = 'timestamp',
    value_col: str = 'consumption_mwh',
    freq: str = 'H',
    test_size: float = 0.2
) -> Tuple[TimeSeries, TimeSeries]:
    """
    Prepare time series data for modeling
    
    Args:
        df: DataFrame containing time series data
        time_col: Name of the timestamp column
        value_col: Name of the value column
        freq: Frequency of the time series
        test_size: Fraction of data to use for testing
        
    Returns:
        Tuple of (train_series, test_series)
    """
    try:
        # Ensure timestamp is datetime
        df[time_col] = pd.to_datetime(df[time_col])
        
        # Create time series
        series = TimeSeries.from_dataframe(
            df,
            time_col=time_col,
            value_cols=value_col,
            freq=freq
        )
        
        # Split into train/test
        train_size = int(len(series) * (1 - test_size))
        train_series, test_series = series[:train_size], series[train_size:]
        
        return train_series, test_series
        
    except Exception as e:
        logger.error(f"Error preparing time series: {str(e)}")
        raise

def train_prophet_model(
    train_series: TimeSeries,
    **kwargs
) -> Tuple[Prophet, TimeSeries]:
    """
    Train a Prophet model on the time series data
    
    Args:
        train_series: Training time series data
        **kwargs: Additional arguments to pass to Prophet
        
    Returns:
        Tuple of (trained_model, predictions_on_train)
    """
    try:
        # Scale the data
        scaler = Scaler()
        scaled_train = scaler.fit_transform(train_series)
        
        # Train Prophet model
        model = Prophet(**kwargs)
        model.fit(scaled_train)
        
        # Make predictions on training data
        predictions = model.predict(len(train_series))
        predictions = scaler.inverse_transform(predictions)
        
        return model, predictions
        
    except Exception as e:
        logger.error(f"Error training Prophet model: {str(e)}")
        raise

def detect_anomalies(
    actual: TimeSeries,
    predicted: TimeSeries,
    threshold_std: float = 2.0
) -> pd.DataFrame:
    """
    Detect anomalies based on prediction errors
    
    Args:
        actual: Actual time series values
        predicted: Predicted time series values
        threshold_std: Number of standard deviations to use as threshold
        
    Returns:
        DataFrame with anomaly information
    """
    try:
        # Calculate errors
        errors = actual - predicted
        
        # Calculate threshold
        mean_error = np.mean(errors.values())
        std_error = np.std(errors.values())
        threshold = threshold_std * std_error
        
        # Detect anomalies
        anomalies = abs(errors.values() - mean_error) > threshold
        
        # Create results DataFrame
        results = pd.DataFrame({
            'timestamp': actual.time_index,
            'actual': actual.values().flatten(),
            'predicted': predicted.values().flatten(),
            'error': errors.values().flatten(),
            'is_anomaly': anomalies.flatten()
        })
        
        return results
        
    except Exception as e:
        logger.error(f"Error detecting anomalies: {str(e)}")
        raise

def evaluate_model(
    model: Prophet,
    train_series: TimeSeries,
    test_series: TimeSeries,
    horizon: int = 24
) -> dict:
    """
    Evaluate a trained model on test data
    
    Args:
        model: Trained model
        train_series: Training time series
        test_series: Test time series
        horizon: Forecast horizon
        
    Returns:
        Dictionary of evaluation metrics
    """
    try:
        # Make predictions
        predictions = model.predict(len(test_series))
        
        # Calculate metrics
        metrics = {
            'mae': mae(test_series, predictions),
            'mse': mse(test_series, predictions),
            'rmse': rmse(test_series, predictions),
            'mape': mape(test_series, predictions)
        }
        
        return metrics
        
    except Exception as e:
        logger.error(f"Error evaluating model: {str(e)}")
        raise
