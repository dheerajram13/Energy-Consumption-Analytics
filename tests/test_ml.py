import pytest
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from sqlalchemy.orm import Session

from src.ml.anomaly_detection import AnomalyDetector
from src.ml.forecasting import EnergyForecaster
from src.ml.service import MLService
from src.models.energy_models import EnergyConsumption, Base
from src.config.database import engine, get_db_session

# Create test database
@pytest.fixture(scope="module")
def test_db():
    # Create all tables
    Base.metadata.create_all(bind=engine)
    
    # Create a test session
    db = Session(engine)
    
    # Add test data
    test_data = []
    start_date = datetime(2023, 1, 1)
    for i in range(100):
        timestamp = start_date + timedelta(hours=i)
        consumption = 100 + 50 * np.sin(i / 24 * 2 * np.pi) + np.random.normal(0, 5)
        test_data.append({
            'timestamp': timestamp,
            'region': 'test_region',
            'consumption_mwh': max(10, consumption),  # Ensure positive consumption
            'temperature': 20 + 10 * np.sin(i / 24 * 2 * np.pi) + np.random.normal(0, 2)
        })
    
    # Add some anomalies
    for i in range(90, 100):
        test_data[i]['consumption_mwh'] += 50  # Add spike
    
    # Insert test data
    for data in test_data:
        db.add(EnergyConsumption(**data))
    
    db.commit()
    
    yield db
    
    # Clean up
    db.close()
    Base.metadata.drop_all(bind=engine)

def test_anomaly_detection(test_db):
    """Test anomaly detection functionality."""
    # Create ML service
    ml_service = MLService(test_db)
    
    # Test dates
    end_date = datetime(2023, 1, 5)  # 5 days of hourly data
    start_date = end_date - timedelta(days=4)
    
    # Test anomaly detection
    result = ml_service.detect_anomalies(
        start_date=start_date,
        end_date=end_date,
        region='test_region',
        model_params={'interval_width': 0.95},
        fit_params={'test_size': 0.2},
        predict_params={'threshold_std': 2.0}
    )
    
    # Check if anomalies were detected
    assert 'anomalies' in result
    assert len(result['anomalies']) > 0
    assert any(a['is_anomaly'] for a in result['anomalies'])

def test_forecasting(test_db):
    """Test forecasting functionality."""
    # Create ML service
    ml_service = MLService(test_db)
    
    # Test dates
    end_date = datetime(2023, 1, 5)  # 5 days of hourly data
    start_date = end_date - timedelta(days=4)
    
    # Test forecasting
    result = ml_service.forecast_consumption(
        start_date=start_date,
        end_date=end_date,
        n_periods=24,  # Forecast next 24 hours
        region='test_region',
        model_type='prophet',
        model_params={
            'daily_seasonality': True,
            'weekly_seasonality': False,
            'yearly_seasonality': False
        },
        fit_params={'test_size': 0.2}
    )
    
    # Check if forecast was generated
    assert 'forecast' in result
    assert len(result['forecast']) == 24
    assert 'metrics' in result
    assert 'mae' in result['metrics']

def test_anomaly_stats(test_db):
    """Test anomaly statistics functionality."""
    # Create ML service
    ml_service = MLService(test_db)
    
    # First, detect some anomalies
    end_date = datetime(2023, 1, 5)
    start_date = end_date - timedelta(days=4)
    
    ml_service.detect_anomalies(
        start_date=start_date,
        end_date=end_date,
        region='test_region'
    )
    
    # Get anomaly stats
    stats = ml_service.get_anomaly_stats(
        start_date=start_date,
        end_date=end_date,
        region='test_region'
    )
    
    # Check if stats were generated
    assert 'total_anomalies' in stats
    assert stats['total_anomalies'] > 0
    assert 'top_regions' in stats
    assert len(stats['top_regions']) > 0
