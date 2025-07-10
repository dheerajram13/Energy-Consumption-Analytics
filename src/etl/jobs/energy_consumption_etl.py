# src/etl/jobs/energy_consumption_etl.py - Complete implementation
import logging
import pandas as pd
from typing import Dict, Any, Optional
from datetime import datetime, timedelta
from pathlib import Path

from ..extractors.smart_meter_extractor import SmartMeterExtractor
from ..extractors.kaggle_extractor import KaggleDataExtractor
from ..transformers.energy_transformer import EnergyConsumptionTransformer
from ..loaders.database import DatabaseLoader
from ..base import ETLPipeline, ETLPipelineBuilder
from ...config.database import get_db_session

logger = logging.getLogger(__name__)

class EnergyConsumptionETLJob:
    """Complete ETL job for energy consumption data"""
    
    def __init__(self, 
                 data_source: str = "simulated",
                 region: str = "default",
                 batch_size: int = 1000):
        """
        Initialize the ETL job
        
        Args:
            data_source: "simulated", "kaggle", or "file"
            region: Region name for the data
            batch_size: Batch size for database loading
        """
        self.data_source = data_source
        self.region = region
        self.batch_size = batch_size
        self.stats = {
            'start_time': None,
            'end_time': None,
            'records_extracted': 0,
            'records_transformed': 0,
            'records_loaded': 0,
            'errors': [],
            'status': 'pending'
        }
    
    def run(self, 
            start_date: str, 
            end_date: str, 
            **kwargs) -> Dict[str, Any]:
        """
        Run the complete ETL pipeline
        
        Args:
            start_date: Start date in YYYY-MM-DD format
            end_date: End date in YYYY-MM-DD format
            **kwargs: Additional parameters
            
        Returns:
            Dictionary with job statistics
        """
        self.stats['start_time'] = datetime.utcnow()
        self.stats['status'] = 'running'
        
        try:
            logger.info(f"Starting ETL job for {start_date} to {end_date}")
            
            # Step 1: Extract data
            logger.info("Starting data extraction...")
            extracted_data = self._extract_data(start_date, end_date, **kwargs)
            self.stats['records_extracted'] = len(extracted_data)
            
            if extracted_data.empty:
                self.stats['status'] = 'no_data'
                logger.warning("No data extracted")
                return self.stats
            
            # Step 2: Transform data
            logger.info("Starting data transformation...")
            transformed_data = self._transform_data(extracted_data)
            self.stats['records_transformed'] = len(transformed_data)
            
            # Step 3: Load data
            logger.info("Starting data loading...")
            load_stats = self._load_data(transformed_data)
            self.stats['records_loaded'] = load_stats.get('records_loaded', 0)
            
            # Step 4: Run data quality checks
            logger.info("Running data quality checks...")
            quality_stats = self._run_quality_checks(transformed_data)
            self.stats['quality_stats'] = quality_stats
            
            self.stats['status'] = 'completed'
            self.stats['end_time'] = datetime.utcnow()
            
            logger.info(f"ETL job completed successfully. "
                       f"Processed {self.stats['records_loaded']} records")
            
            return self.stats
            
        except Exception as e:
            self.stats['status'] = 'failed'
            self.stats['end_time'] = datetime.utcnow()
            self.stats['errors'].append({
                'error': str(e),
                'timestamp': datetime.utcnow().isoformat()
            })
            
            logger.error(f"ETL job failed: {str(e)}", exc_info=True)
            raise
    
    def _extract_data(self, start_date: str, end_date: str, **kwargs) -> pd.DataFrame:
        """Extract data based on configured source"""
        
        if self.data_source == "simulated":
            extractor = SmartMeterExtractor(simulate_data=True)
            return extractor.extract(
                start_date, 
                end_date, 
                num_meters=kwargs.get('num_meters', 50)
            )
            
        elif self.data_source == "kaggle":
            dataset_name = kwargs.get('kaggle_dataset', 'unitednations/global-commodity-trade-statistics')
            extractor = KaggleDataExtractor(dataset_name)
            return extractor.extract()
            
        elif self.data_source == "file":
            file_path = kwargs.get('file_path')
            if not file_path:
                raise ValueError("file_path must be provided for file data source")
            
            if not Path(file_path).exists():
                raise FileNotFoundError(f"File not found: {file_path}")
            
            return pd.read_csv(file_path)
            
        else:
            raise ValueError(f"Unsupported data source: {self.data_source}")
    
    def _transform_data(self, df: pd.DataFrame) -> list:
        """Transform the extracted data"""
        
        transformer = EnergyConsumptionTransformer(
            region=self.region,
            min_consumption=0.0,
            max_consumption=10000.0
        )
        
        # Convert DataFrame to list of dicts for transformer
        data_dicts = df.to_dict('records')
        
        # Transform data
        transformed_data = transformer.transform(data_dicts)
        
        # Log transformation statistics
        transform_stats = transformer.get_transformation_stats()
        logger.info(f"Transformation stats: {transform_stats}")
        
        return transformed_data
    
    def _load_data(self, data: list) -> Dict[str, Any]:
        """Load transformed data to database"""
        
        # Get database session
        db_session = next(get_db_session())
        
        try:
            # Initialize loader
            loader = DatabaseLoader(db_session, batch_size=self.batch_size)
            
            # Load data
            load_stats = loader.load(data)
            
            return load_stats
            
        finally:
            db_session.close()
    
    def _run_quality_checks(self, data: list) -> Dict[str, Any]:
        """Run data quality checks on transformed data"""
        
        df = pd.DataFrame(data)
        
        quality_stats = {
            'total_records': len(df),
            'null_percentages': {},
            'duplicate_count': 0,
            'data_types': {},
            'value_ranges': {}
        }
        
        # Check for nulls
        for col in df.columns:
            null_pct = (df[col].isnull().sum() / len(df)) * 100
            quality_stats['null_percentages'][col] = round(null_pct, 2)
        
        # Check for duplicates
        quality_stats['duplicate_count'] = df.duplicated().sum()
        
        # Check data types
        quality_stats['data_types'] = df.dtypes.astype(str).to_dict()
        
        # Check value ranges for numeric columns
        numeric_cols = df.select_dtypes(include=['number']).columns
        for col in numeric_cols:
            quality_stats['value_ranges'][col] = {
                'min': float(df[col].min()),
                'max': float(df[col].max()),
                'mean': float(df[col].mean())
            }
        
        return quality_stats

# Convenience function for Airflow
def run_energy_consumption_etl(
    start_date: str,
    end_date: str,
    data_source: str = "simulated",
    region: str = "default",
    **kwargs
) -> Dict[str, Any]:
    """
    Run energy consumption ETL job
    
    This function can be called from Airflow DAGs or standalone scripts
    
    Args:
        start_date: Start date in YYYY-MM-DD format
        end_date: End date in YYYY-MM-DD format
        data_source: Data source type
        region: Region name
        **kwargs: Additional parameters
        
    Returns:
        Job statistics dictionary
    """
    
    job = EnergyConsumptionETLJob(
        data_source=data_source,
        region=region,
        batch_size=kwargs.get('batch_size', 1000)
    )
    
    return job.run(start_date, end_date, **kwargs)

# Example usage for testing
if __name__ == "__main__":
    # Test the ETL job
    from datetime import date
    
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # Run ETL for yesterday
    yesterday = date.today() - timedelta(days=1)
    today = date.today()
    
    try:
        stats = run_energy_consumption_etl(
            start_date=yesterday.strftime('%Y-%m-%d'),
            end_date=today.strftime('%Y-%m-%d'),
            data_source="simulated",
            region="test_region",
            num_meters=10
        )
        
        print("ETL Job completed successfully!")
        print(f"Statistics: {stats}")
        
    except Exception as e:
        print(f"ETL Job failed: {str(e)}")
        raise