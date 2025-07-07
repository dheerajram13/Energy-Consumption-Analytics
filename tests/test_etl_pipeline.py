""
Test ETL Pipeline

This module contains tests for the ETL pipeline.
"""
import os
import sys
import pytest
from datetime import datetime, timedelta
from unittest.mock import MagicMock, patch
import pandas as pd
import numpy as np

# Add the project root to the Python path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.etl.extractors.base import BaseExtractor
from src.etl.transformers.energy_transformer import EnergyConsumptionTransformer
from src.etl.loaders.database import DatabaseLoader
from src.etl.base import ETLPipeline, ETLPipelineBuilder
from src.models.energy_models import EnergyConsumption, Anomaly, Base
from src.config.database import get_db_session, engine

# Test data
def generate_test_data(n: int = 10) -> list[dict]:
    """Generate test data for the ETL pipeline."""
    base_time = datetime.utcnow()
    data = []
    
    for i in range(n):
        timestamp = base_time - timedelta(hours=i)
        data.append({
            'timestamp': timestamp.isoformat(),
            'consumption_mwh': 100.0 + (i * 0.5) + (np.random.rand() * 10 - 5),
            'region': 'test_region',
            'temperature': 20.0 + (np.random.rand() * 10 - 5),
            'is_holiday': 1 if i % 7 == 0 else 0  # Every 7th record is a holiday
        })
    
    return data

# Fixtures
@pytest.fixture(scope="module")
def db_session():
    """Create a clean database session for testing."""
    # Create all tables
    Base.metadata.create_all(bind=engine)
    
    # Create a new session for testing
    session = get_db_session()
    
    yield session
    
    # Clean up after tests
    session.close()
    Base.metadata.drop_all(bind=engine)

@pytest.fixture
def test_data() -> list[dict]:
    """Generate test data."""
    return generate_test_data(100)

@pytest.fixture
extractor():
    """Create a mock extractor for testing."""
    class TestExtractor(BaseExtractor):
        def extract(self, *args, **kwargs):
            return generate_test_data(10)
    
    return TestExtractor()

@pytest.fixture
transformer():
    """Create a transformer for testing."""
    return EnergyConsumptionTransformer(region='test_region')

@pytest.fixture
def loader(db_session):
    """Create a database loader for testing."""
    return DatabaseLoader(db_session, batch_size=5)

# Tests
def test_extractor(extractor):
    """Test the extractor."""
    data = extractor.extract()
    assert isinstance(data, list)
    assert len(data) == 10
    assert all(isinstance(item, dict) for item in data)
    assert all('consumption_mwh' in item for item in data)

def test_transformer(transformer, test_data):
    """Test the transformer."""
    transformed = transformer.transform(test_data)
    
    assert isinstance(transformed, list)
    assert len(transformed) == len(test_data)
    
    # Check that the transformed data has the expected fields
    for item in transformed:
        assert 'timestamp' in item
        assert 'consumption_mwh' in item
        assert 'region' in item
        assert 'temperature' in item
        assert 'is_holiday' in item
        assert 'hour_of_day' in item
        assert 'day_of_week' in item
        assert 'is_weekend' in item
        assert 'month' in item
        assert 'year' in item
        assert 'season' in item
        assert 'data_quality_score' in item

def test_loader(loader, test_data, transformer):
    """Test the database loader."""
    # Transform the test data first
    transformed = transformer.transform(test_data)
    
    # Load the data
    result = loader.load(transformed)
    
    # Check that the load was successful
    assert result is True
    
    # Verify the data was loaded into the database
    session = loader.db
    count = session.query(EnergyConsumption).count()
    assert count == len(test_data)

def test_etl_pipeline(extractor, transformer, loader):
    """Test the entire ETL pipeline."""
    # Create the pipeline
    pipeline = ETLPipeline(
        extractor=extractor,
        transformer=transformer,
        loader=loader,
        name="test_pipeline"
    )
    
    # Run the pipeline
    result = pipeline.run()
    
    # Check that the pipeline completed successfully
    assert result is True
    
    # Get statistics
    stats = pipeline.get_stats()
    
    # Check that the stats have the expected structure
    assert 'status' in stats
    assert 'start_time' in stats
    assert 'end_time' in stats
    assert 'duration_seconds' in stats
    assert 'extractor' in stats
    assert 'transformer' in stats
    assert 'loader' in stats
    assert 'name' in stats
    
    # Check that the data was loaded into the database
    session = loader.db
    count = session.query(EnergyConsumption).count()
    assert count > 0

def test_etl_pipeline_builder():
    """Test the ETL pipeline builder."""
    # Create a mock extractor, transformer, and loader
    mock_extractor = MagicMock(spec=BaseExtractor)
    mock_transformer = MagicMock()
    mock_loader = MagicMock()
    
    # Configure the mocks
    mock_extractor.extract.return_value = [{'test': 'data'}]
    mock_transformer.transform.return_value = [{'transformed': 'data'}]
    mock_loader.load.return_value = True
    
    # Build the pipeline
    pipeline = (
        ETLPipelineBuilder()
        .with_extractor(mock_extractor)
        .with_transformer(mock_transformer)
        .with_loader(mock_loader)
        .with_name("test_pipeline")
        .build()
    )
    
    # Run the pipeline
    result = pipeline.run()
    
    # Check that the pipeline completed successfully
    assert result is True
    
    # Verify that the extract, transform, and load methods were called
    mock_extractor.extract.assert_called_once()
    mock_transformer.transform.assert_called_once_with([{'test': 'data'}])
    mock_loader.load.assert_called_once_with([{'transformed': 'data'}])

# Run the tests
if __name__ == "__main__":
    pytest.main([__file__, "-v"])
