import pandas as pd
import numpy as np
from typing import Dict, Any, Optional, List, Tuple
from datetime import datetime, timedelta
from sqlalchemy.orm import Session
import logging

from .anomaly_detection import AnomalyDetector
from .forecasting import EnergyForecaster
from ..models.energy_models import EnergyConsumption, Anomaly
from ..config.database import get_db_session

logger = logging.getLogger(__name__)

class MLService:
    """
    Service class for ML operations.
    """
    
    def __init__(self, db: Session):
        """
        Initialize the ML service.
        
        Args:
            db: Database session
        """
        self.db = db
        self.anomaly_detector = None
        self.forecaster = None
    
    def load_consumption_data(self, start_date: datetime, end_date: datetime, 
                             region: Optional[str] = None) -> pd.DataFrame:
        """
        Load energy consumption data from the database.
        
        Args:
            start_date: Start date for the data
            end_date: End date for the data
            region: Optional region filter
            
        Returns:
            DataFrame with the consumption data
        """
        try:
            query = self.db.query(EnergyConsumption).filter(
                EnergyConsumption.timestamp >= start_date,
                EnergyConsumption.timestamp <= end_date
            )
            
            if region:
                query = query.filter(EnergyConsumption.region == region)
            
            # Execute query and convert to DataFrame
            df = pd.read_sql(query.statement, self.db.bind)
            
            return df
            
        except Exception as e:
            logger.error(f"Error loading consumption data: {str(e)}")
            raise
    
    def detect_anomalies(self, start_date: datetime, end_date: datetime, 
                         region: Optional[str] = None, **kwargs) -> Dict[str, Any]:
        """
        Detect anomalies in energy consumption data.
        
        Args:
            start_date: Start date for the data
            end_date: End date for the data
            region: Optional region filter
            **kwargs: Additional arguments for the anomaly detector
            
        Returns:
            Dictionary with anomalies and model metrics
        """
        try:
            # Load data
            df = self.load_consumption_data(start_date, end_date, region)
            
            if df.empty:
                return {"error": "No data found for the specified criteria"}
            
            # Initialize and train anomaly detector
            self.anomaly_detector = AnomalyDetector(**kwargs.get('model_params', {}))
            self.anomaly_detector.fit(df, **kwargs.get('fit_params', {}))
            
            # Detect anomalies
            anomalies = self.anomaly_detector.predict(df, **kwargs.get('predict_params', {}))
            
            # Save anomalies to database
            self._save_anomalies(anomalies, region)
            
            # Get evaluation metrics
            metrics = self.anomaly_detector.evaluate()
            
            return {
                'anomalies': anomalies.to_dict('records'),
                'metrics': metrics
            }
            
        except Exception as e:
            logger.error(f"Error detecting anomalies: {str(e)}")
            raise
    
    def forecast_consumption(self, start_date: datetime, end_date: datetime,
                            n_periods: int = 24, region: Optional[str] = None,
                            model_type: str = 'prophet', **kwargs) -> Dict[str, Any]:
        """
        Forecast energy consumption.
        
        Args:
            start_date: Start date for the training data
            end_date: End date for the training data
            n_periods: Number of periods to forecast
            region: Optional region filter
            model_type: Type of model to use ('prophet', 'exponential_smoothing', 'arima')
            **kwargs: Additional arguments for the forecaster
            
        Returns:
            Dictionary with forecasts and model metrics
        """
        try:
            # Load data
            df = self.load_consumption_data(start_date, end_date, region)
            
            if df.empty:
                return {"error": "No data found for the specified criteria"}
            
            # Initialize and train forecaster
            self.forecaster = EnergyForecaster(
                model_type=model_type,
                model_params=kwargs.get('model_params', {})
            )
            self.forecaster.fit(df, **kwargs.get('fit_params', {}))
            
            # Make forecasts
            forecast = self.forecaster.predict(n_periods)
            
            # Get evaluation metrics
            metrics = self.forecaster.evaluate()
            
            # Convert forecast to DataFrame
            forecast_df = pd.DataFrame({
                'timestamp': forecast.time_index,
                'forecast': forecast.values().flatten()
            })
            
            return {
                'forecast': forecast_df.to_dict('records'),
                'metrics': metrics
            }
            
        except Exception as e:
            logger.error(f"Error forecasting consumption: {str(e)}")
            raise
    
    def _save_anomalies(self, anomalies_df: pd.DataFrame, region: Optional[str] = None) -> None:
        """
        Save detected anomalies to the database.
        
        Args:
            anomalies_df: DataFrame with anomaly data
            region: Optional region for the anomalies
        """
        try:
            # Only process rows marked as anomalies
            anomalies = anomalies_df[anomalies_df['is_anomaly']]
            
            for _, row in anomalies.iterrows():
                anomaly = Anomaly(
                    timestamp=row['timestamp'],
                    region=region,
                    actual_value=row['actual'],
                    predicted_value=row['predicted'],
                    anomaly_score=row['error'],
                    is_confirmed=0  # 0 for unconfirmed, 1 for confirmed, -1 for false positive
                )
                self.db.add(anomaly)
            
            self.db.commit()
            
        except Exception as e:
            self.db.rollback()
            logger.error(f"Error saving anomalies: {str(e)}")
            raise
    
    def get_anomaly_stats(self, start_date: datetime, end_date: datetime,
                          region: Optional[str] = None) -> Dict[str, Any]:
        """
        Get statistics about anomalies.
        
        Args:
            start_date: Start date for the data
            end_date: End date for the data
            region: Optional region filter
            
        Returns:
            Dictionary with anomaly statistics
        """
        try:
            query = self.db.query(Anomaly).filter(
                Anomaly.timestamp >= start_date,
                Anomaly.timestamp <= end_date
            )
            
            if region:
                query = query.filter(Anomaly.region == region)
            
            # Get all anomalies
            anomalies = query.all()
            
            if not anomalies:
                return {"message": "No anomalies found for the specified criteria"}
            
            # Calculate statistics
            total_anomalies = len(anomalies)
            confirmed = sum(1 for a in anomalies if a.is_confirmed == 1)
            false_positives = sum(1 for a in anomalies if a.is_confirmed == -1)
            unconfirmed = total_anomalies - confirmed - false_positives
            
            avg_anomaly_score = sum(a.anomaly_score for a in anomalies) / total_anomalies
            
            # Get top regions with most anomalies
            region_stats = {}
            for anomaly in anomalies:
                region = anomaly.region or 'unknown'
                if region not in region_stats:
                    region_stats[region] = 0
                region_stats[region] += 1
            
            top_regions = [
                {"region": region, "count": count}
                for region, count in sorted(
                    region_stats.items(), 
                    key=lambda x: x[1], 
                    reverse=True
                )[:5]  # Top 5 regions
            ]
            
            return {
                "total_anomalies": total_anomalies,
                "confirmed_anomalies": confirmed,
                "false_positives": false_positives,
                "unconfirmed_anomalies": unconfirmed,
                "avg_anomaly_score": avg_anomaly_score,
                "top_regions": top_regions
            }
            
        except Exception as e:
            logger.error(f"Error getting anomaly stats: {str(e)}")
            raise
