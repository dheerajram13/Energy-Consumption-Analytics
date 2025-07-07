"""
Energy ETL Job

This module contains the main ETL pipeline for energy data processing.
"""
import logging
from datetime import datetime
from typing import Dict, Any, Optional
import pandas as pd
from sqlalchemy.orm import Session

from src.etl.extractors.base import BaseExtractor
from src.etl.transformers.energy_transformer import EnergyConsumptionTransformer
from src.etl.loaders.database import DatabaseLoader
from src.config.database import get_db_session

logger = logging.getLogger(__name__)

class EnergyETL:
    """Main ETL class for energy data processing."""
    
    def __init__(self, db_session: Optional[Session] = None):
        """Initialize the ETL pipeline."""
        self.db_session = db_session or get_db_session()
        self.extractor = BaseExtractor()
        self.transformer = EnergyConsumptionTransformer()
        self.loader = DatabaseLoader(self.db_session)

    def run(self, start_date: str, end_date: str) -> Dict[str, Any]:
        """Run the ETL pipeline.
        
        Args:
            start_date: Start date in 'YYYY-MM-DD' format
            end_date: End date in 'YYYY-MM-DD' format
            
        Returns:
            Dict containing ETL statistics
        """
        stats = {
            'start_time': datetime.utcnow(),
            'records_processed': 0,
            'errors': [],
            'status': 'success'
        }
        
        try:
            # Extract data
            logger.info(f"Extracting data from {start_date} to {end_date}")
            raw_data = self.extractor.extract(start_date, end_date)
            
            if raw_data.empty:
                logger.warning("No data extracted for the given date range")
                stats['status'] = 'no_data'
                return stats
            
            # Transform data
            logger.info("Transforming data")
            transformed_data = self.transformer.transform(raw_data)
            
            # Load data
            logger.info("Loading data to database")
            load_stats = self.loader.load(transformed_data)
            
            # Update stats
            stats.update({
                'end_time': datetime.utcnow(),
                'records_processed': len(transformed_data),
                'load_stats': load_stats,
            })
            
            logger.info(f"ETL completed successfully. Processed {len(transformed_data)} records.")
            
        except Exception as e:
            logger.error(f"ETL failed: {str(e)}", exc_info=True)
            stats.update({
                'end_time': datetime.utcnow(),
                'status': 'failed',
                'error': str(e)
            })
            
        return stats

def run_etl_pipeline(start_date: str, end_date: str) -> Dict[str, Any]:
    """Run the ETL pipeline.
    
    This function is called by the Airflow DAG.
    
    Args:
        start_date: Start date in 'YYYY-MM-DD' format
        end_date: End date in 'YYYY-MM-DD' format
        
    Returns:
        Dict containing ETL statistics
    """
    etl = EnergyETL()
    return etl.run(start_date, end_date)
