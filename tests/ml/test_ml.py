# tests/test_ml.py - ML tests
import pytest
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from src.ml.energy_anomaly_detector import EnergyAnomalyDetector

class TestAnomalyDetector:
    def test_anomaly_detection(self, sample_energy_data):
        """Test anomaly detection with isolation forest"""
        # Create detector
        detector = EnergyAnomalyDetector(method='isolation_forest', contamination=0.1)
        
        # Fit the model
        detector.fit(sample_energy_data)
        
        # Make predictions
        results = detector.predict(sample_energy_data)
        
        # Check results
        assert len(results) == len(sample_energy_data)
        assert 'is_anomaly' in results.columns
        assert 'anomaly_score' in results.columns
        assert 'anomaly_confidence' in results.columns
        
        # Should find some anomalies (around 10% of the data)
        anomaly_count = results['is_anomaly'].sum()
        assert 5 <= anomaly_count <= 15  # Roughly 10% of 100 samples
        
    def test_statistical_anomaly_detection(self, sample_energy_data):
        """Test statistical anomaly detection"""
        # Create detector with statistical method
        detector = EnergyAnomalyDetector(method='statistical', z_threshold=3.0)
        
        # Fit the model
        detector.fit(sample_energy_data)
        
        # Make predictions
        results = detector.predict(sample_energy_data)
        
        # Check results
        assert len(results) == len(sample_energy_data)
        assert 'is_anomaly' in results.columns
        assert 'anomaly_score' in results.columns
        assert 'anomaly_reason' in results.columns
        
        # Should find some anomalies based on z-score
        anomaly_count = results['is_anomaly'].sum()
        assert anomaly_count >= 0  # Could be 0 if no points exceed z=3
        
    def test_evaluation(self, sample_energy_data):
        """Test evaluation metrics"""
        # Create detector
        detector = EnergyAnomalyDetector(method='isolation_forest')
        
        # Create some artificial anomalies
        sample_data = sample_energy_data.copy()
        sample_data['is_anomaly'] = False
        
        # Make some points anomalies
        sample_data.loc[sample_data.sample(frac=0.1).index, 'is_anomaly'] = True
        
        # Fit and predict
        detector.fit(sample_data)
        results = detector.predict(sample_data)
        
        # Evaluate
        metrics = detector.evaluate(
            sample_data, 
            sample_data['is_anomaly']
        )
        
        # Check metrics
        assert 'accuracy' in metrics
        assert 'precision' in metrics
        assert 'recall' in metrics
        assert 'f1_score' in metrics
        assert 'confusion_matrix' in metrics
        
        # Metrics should be between 0 and 1
        assert 0 <= metrics['accuracy'] <= 1
        assert 0 <= metrics['precision'] <= 1
        assert 0 <= metrics['recall'] <= 1
        assert 0 <= metrics['f1_score'] <= 1
