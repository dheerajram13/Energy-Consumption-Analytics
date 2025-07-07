"""
Database Loader

This module contains the database loader for saving transformed data to the database.
"""
from typing import List, Dict, Any, Optional
import logging
from sqlalchemy.orm import Session
from sqlalchemy.exc import SQLAlchemyError
from datetime import datetime

from src.models.energy_models import EnergyConsumption, Anomaly, Base

logger = logging.getLogger(__name__)

class DatabaseLoader:
    """Loads data into the database."""
    
    def __init__(self, db_session: Session, batch_size: int = 1000):
        """Initialize the database loader.
        
        Args:
            db_session: SQLAlchemy database session
            batch_size: Number of records to insert in a single transaction
        """
        self.db = db_session
        self.batch_size = batch_size
    
    def load(self, data: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Load transformed data into the database.
        
        Args:
            data: List of transformed data dictionaries
            
        Returns:
            Dictionary with load statistics
        """
        if not data:
            logger.warning("No data to load")
            return {'records_loaded': 0, 'errors': []}
        
        stats = {
            'start_time': datetime.utcnow(),
            'records_processed': 0,
            'records_loaded': 0,
            'errors': []
        }
        
        try:
            # Process data in batches
            for i in range(0, len(data), self.batch_size):
                batch = data[i:i + self.batch_size]
                self._process_batch(batch, stats)
                
            # Commit any remaining transactions
            self.db.commit()
            
            stats.update({
                'end_time': datetime.utcnow(),
                'status': 'success'
            })
            
            logger.info(
                f"Successfully loaded {stats['records_loaded']} records "
                f"in {self._format_duration(stats['start_time'], stats['end_time'])}"
            )
            
        except Exception as e:
            self.db.rollback()
            stats.update({
                'end_time': datetime.utcnow(),
                'status': 'failed',
                'error': str(e)
            })
            logger.error(f"Failed to load data: {str(e)}", exc_info=True)
            
        return stats
    
    def _process_batch(self, batch: List[Dict[str, Any]], stats: Dict[str, Any]) -> None:
        """Process a batch of records."""
        try:
            for record in batch:
                self._process_record(record, stats)
                
            # Commit after each successful batch
            self.db.commit()
            
        except Exception as e:
            self.db.rollback()
            stats['errors'].append({
                'error': str(e),
                'batch_size': len(batch)
            })
            logger.error(f"Error processing batch: {str(e)}")
            raise
    
    def _process_record(self, record: Dict[str, Any], stats: Dict[str, Any]) -> None:
        """Process a single record."""
        try:
            # Create or update energy consumption record
            energy_record = EnergyConsumption(
                timestamp=record['timestamp'],
                consumption_mwh=record['consumption_mwh'],
                region=record['region'],
                temperature=record.get('temperature'),
                is_holiday=record.get('is_holiday', False),
                hour_of_day=record.get('hour_of_day'),
                day_of_week=record.get('day_of_week'),
                is_weekend=record.get('is_weekend'),
                month=record.get('month'),
                year=record.get('year'),
                season=record.get('season'),
                data_quality_score=record.get('data_quality_score', 1.0)
            )
            
            self.db.add(energy_record)
            self.db.flush()  # Flush to get the ID for the anomaly record if needed
            
            # If this record was marked as an anomaly, create an anomaly record
            if record.get('is_anomaly'):
                anomaly = Anomaly(
                    energy_consumption_id=energy_record.id,
                    timestamp=record['timestamp'],
                    region=record['region'],
                    original_value=record['consumption_mwh'],
                    predicted_value=record.get('predicted_value'),
                    anomaly_score=record.get('anomaly_score', 0.0),
                    method=record.get('anomaly_method', 'unknown'),
                    metadata=record.get('anomaly_metadata', {})
                )
                self.db.add(anomaly)
            
            stats['records_processed'] += 1
            stats['records_loaded'] += 1
            
        except Exception as e:
            stats['errors'].append({
                'record': record,
                'error': str(e)
            })
            logger.warning(f"Error processing record: {str(e)}")
            raise
    
    @staticmethod
    def _format_duration(start: datetime, end: datetime) -> str:
        """Format duration between two datetimes as a human-readable string."""
        duration = end - start
        total_seconds = int(duration.total_seconds())
        
        if total_seconds < 60:
            return f"{total_seconds} seconds"
        elif total_seconds < 3600:
            minutes = total_seconds // 60
            seconds = total_seconds % 60
            return f"{minutes}m {seconds}s"
        else:
            hours = total_seconds // 3600
            minutes = (total_seconds % 3600) // 60
            return f"{hours}h {minutes}m"
    
    def __del__(self):
        """Ensure the database session is closed when the loader is destroyed."""
        try:
            self.db.close()
        except:
            pass
