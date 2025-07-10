import pandas as pd
import numpy as np
from typing import Dict, Any, Optional, Tuple, List
from datetime import datetime
from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import classification_report, confusion_matrix
import logging

logger = logging.getLogger(__name__)

class EnergyAnomalyDetector:
    """Complete anomaly detection for energy consumption data"""
    
    def __init__(self, method: str = 'isolation_forest', **kwargs):
        """
        Initialize the anomaly detector
        
        Args:
            method: 'isolation_forest', 'statistical', or 'hybrid'
            **kwargs: Method-specific parameters
        """
        self.method = method
        self.model = None
        self.scaler = StandardScaler()
        self.feature_columns = []
        self.threshold = None
        self.is_fitted = False
        
        # Method-specific parameters
        if method == 'isolation_forest':
            self.contamination = kwargs.get('contamination', 0.1)
            self.n_estimators = kwargs.get('n_estimators', 100)
            self.random_state = kwargs.get('random_state', 42)
        elif method == 'statistical':
            self.z_threshold = kwargs.get('z_threshold', 3.0)
            self.window_size = kwargs.get('window_size', 24)  # 24 hours
        
    def _create_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """Create features for anomaly detection"""
        features = df.copy()
        
        # Time-based features
        features['hour'] = features['timestamp'].dt.hour
        features['day_of_week'] = features['timestamp'].dt.dayofweek
        features['month'] = features['timestamp'].dt.month
        features['is_weekend'] = features['day_of_week'].isin([5, 6]).astype(int)
        
        # Lag features (previous consumption values)
        features['consumption_lag_1'] = features['consumption_mwh'].shift(1)
        features['consumption_lag_24'] = features['consumption_mwh'].shift(24)  # Same hour yesterday
        features['consumption_lag_168'] = features['consumption_mwh'].shift(168)  # Same hour last week
        
        # Rolling statistics
        features['rolling_mean_24h'] = features['consumption_mwh'].rolling(window=24, min_periods=1).mean()
        features['rolling_std_24h'] = features['consumption_mwh'].rolling(window=24, min_periods=1).std()
        features['rolling_mean_7d'] = features['consumption_mwh'].rolling(window=168, min_periods=1).mean()
        
        # Consumption ratios
        features['consumption_vs_daily_avg'] = features['consumption_mwh'] / features['rolling_mean_24h']
        features['consumption_vs_weekly_avg'] = features['consumption_mwh'] / features['rolling_mean_7d']
        
        # Temperature features (if available)
        if 'temperature' in features.columns:
            features['temp_consumption_ratio'] = features['consumption_mwh'] / (features['temperature'] + 1e-6)
        
        # Select numeric features only
        numeric_features = features.select_dtypes(include=[np.number]).columns.tolist()
        
        # Remove target variable and ID columns
        exclude_cols = ['consumption_mwh', 'meter_id', 'region']
        self.feature_columns = [col for col in numeric_features if col not in exclude_cols]
        
        return features[self.feature_columns].fillna(0)
    
    def fit(self, df: pd.DataFrame) -> 'EnergyAnomalyDetector':
        """Fit the anomaly detection model"""
        try:
            # Create features
            X = self._create_features(df)
            
            if self.method == 'isolation_forest':
                # Scale features
                X_scaled = self.scaler.fit_transform(X)
                
                # Train Isolation Forest
                self.model = IsolationForest(
                    contamination=self.contamination,
                    n_estimators=self.n_estimators,
                    random_state=self.random_state,
                    n_jobs=-1
                )
                self.model.fit(X_scaled)
                
            elif self.method == 'statistical':
                # Calculate statistical thresholds for each feature
                self.stats = {
                    'mean': X.mean(),
                    'std': X.std(),
                    'q1': X.quantile(0.25),
                    'q3': X.quantile(0.75)
                }
                self.stats['iqr'] = self.stats['q3'] - self.stats['q1']
                
            self.is_fitted = True
            logger.info(f"Anomaly detector fitted with method: {self.method}")
            return self
            
        except Exception as e:
            logger.error(f"Error fitting anomaly detector: {str(e)}")
            raise
    
    def predict(self, df: pd.DataFrame) -> pd.DataFrame:
        """Detect anomalies in the data"""
        if not self.is_fitted:
            raise ValueError("Model must be fitted before prediction")
        
        try:
            # Create features
            X = self._create_features(df)
            
            # Make predictions
            if self.method == 'isolation_forest':
                X_scaled = self.scaler.transform(X)
                anomaly_scores = self.model.decision_function(X_scaled)
                is_anomaly = self.model.predict(X_scaled) == -1
                
            elif self.method == 'statistical':
                # Z-score based detection
                z_scores = np.abs((X - self.stats['mean']) / self.stats['std'])
                anomaly_scores = z_scores.max(axis=1)  # Max z-score across features
                is_anomaly = anomaly_scores > self.z_threshold
            
            # Create results dataframe
            results = df.copy()
            results['anomaly_score'] = anomaly_scores
            results['is_anomaly'] = is_anomaly
            results['anomaly_confidence'] = np.abs(anomaly_scores)
            
            # Add anomaly reasons for interpretability
            if self.method == 'statistical':
                results['anomaly_reason'] = self._get_anomaly_reasons(X, z_scores)
            
            return results
            
        except Exception as e:
            logger.error(f"Error predicting anomalies: {str(e)}")
            raise
    
    def _get_anomaly_reasons(self, X: pd.DataFrame, z_scores: pd.DataFrame) -> List[str]:
        """Get human-readable reasons for anomalies"""
        reasons = []
        
        for idx in range(len(X)):
            row_z_scores = z_scores.iloc[idx]
            max_z_feature = row_z_scores.idxmax()
            max_z_value = row_z_scores.max()
            
            if max_z_value > self.z_threshold:
                reasons.append(f"High {max_z_feature} (z-score: {max_z_value:.2f})")
            else:
                reasons.append("Normal")
        
        return reasons
    
    def evaluate(self, df: pd.DataFrame, true_anomalies: pd.Series) -> Dict[str, Any]:
        """Evaluate the anomaly detector if ground truth is available"""
        predictions = self.predict(df)
        predicted_anomalies = predictions['is_anomaly']
        
        # Calculate metrics
        report = classification_report(true_anomalies, predicted_anomalies, output_dict=True)
        cm = confusion_matrix(true_anomalies, predicted_anomalies)
        
        return {
            'classification_report': report,
            'confusion_matrix': cm.tolist(),
            'accuracy': report['accuracy'],
            'precision': report['True']['precision'] if 'True' in report else 0,
            'recall': report['True']['recall'] if 'True' in report else 0,
            'f1_score': report['True']['f1-score'] if 'True' in report else 0
        }
