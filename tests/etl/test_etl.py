# tests/test_etl.py - ETL tests
import pytest
from datetime import datetime
from src.etl.extractors.smart_meter_extractor import SmartMeterExtractor
from src.etl.transformers.energy_transformer import EnergyConsumptionTransformer
from src.etl.loaders.database import DatabaseLoader

class TestSmartMeterExtractor:
    def test_extract_with_simulation(self):
        """Test smart meter data extraction with simulation"""
        extractor = SmartMeterExtractor(simulate_data=True)
        df = extractor.extract("2023-01-01", "2023-01-02", num_meters=5)
        
        assert len(df) > 0
        assert 'timestamp' in df.columns
        assert 'consumption_mwh' in df.columns
        assert 'meter_id' in df.columns
        assert 'region' in df.columns
        assert 'temperature' in df.columns
        assert 'is_holiday' in df.columns

class TestEnergyTransformer:
    def test_transform_energy_data(self, sample_energy_data):
        """Test energy data transformation"""
        transformer = EnergyConsumptionTransformer(region="test_region")
        
        # Convert to list of dicts
        data_dicts = sample_energy_data.to_dict('records')
        
        # Transform
        transformed_data = transformer.transform(data_dicts)
        
        assert len(transformed_data) == len(data_dicts)
        assert all('timestamp' in record for record in transformed_data)
        assert all('region' in record for record in transformed_data)
        assert all('consumption_mwh' in record for record in transformed_data)
        assert all('temperature' in record for record in transformed_data)

class TestDatabaseLoader:
    def test_load_data(self, test_db, sample_energy_data):
        """Test loading data into the database"""
        loader = DatabaseLoader(db_session=test_db, batch_size=10)
        
        # Convert to list of dicts
        data_dicts = sample_energy_data.to_dict('records')
        
        # Load data
        stats = loader.load(data_dicts)
        
        assert stats['status'] == 'success'
        assert stats['records_processed'] == len(data_dicts)
        assert stats['records_loaded'] == len(data_dicts)
        
        # Verify data was loaded
        count = test_db.query(EnergyConsumption).count()
        assert count == len(data_dicts)
