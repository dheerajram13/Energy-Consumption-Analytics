"""
Energy Data Transformer

This module contains transformers for processing energy consumption data.
"""
from typing import List, Dict, Any, Optional, Tuple
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
import logging
from ..base import BaseTransformer

logger = logging.getLogger(__name__)

class EnergyConsumptionTransformer(BaseTransformer):
    """Transformer for energy consumption data."""
    
    def __init__(self, region: Optional[str] = None, min_consumption: float = 0.0, max_consumption: float = 100000.0):
        """Initialize the transformer.
        
        Args:
            region: Optional region filter
            min_consumption: Minimum valid consumption value (MWh)
            max_consumption: Maximum valid consumption value (MWh)
        """
        self.region = region
        self.min_consumption = min_consumption
        self.max_consumption = max_consumption
        self.stats = {
            'total_records': 0,
            'transformed_records': 0,
            'invalid_records': 0,
            'errors': []
        }
    
    def transform(self, data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Transform raw energy consumption data.
        
        Args:
            data: List of dictionaries containing raw data
            
        Returns:
            List of transformed data dictionaries
        """
        self.stats['total_records'] = len(data)
        transformed = []
        
        for row in data:
            try:
                # Convert and validate data types
                timestamp = self._parse_timestamp(row.get('timestamp'))
                consumption = self._validate_consumption(row.get('consumption_mwh'))
                temperature = self._parse_float(row.get('temperature'), default=0.0)
                is_holiday = bool(int(row.get('is_holiday', 0)))
                
                transformed_row = {
                    'timestamp': timestamp,
                    'consumption_mwh': consumption,
                    'region': self.region or row.get('region', 'unknown').lower().strip(),
                    'temperature': temperature,
                    'is_holiday': is_holiday,
                    'is_anomaly': False,  # Will be set by anomaly detection
                    'data_quality_score': self._calculate_quality_score(row)
                }
                
                # Add derived features
                transformed_row.update(self._add_derived_features(transformed_row))
                
                transformed.append(transformed_row)
                self.stats['transformed_records'] += 1
                
            except (ValueError, KeyError, TypeError) as e:
                self.stats['invalid_records'] += 1
                self.stats['errors'].append({
                    'row': row,
                    'error': str(e)
                })
                logger.warning(f"Error transforming row: {str(e)}")
                continue
        
        logger.info(f"Transformation complete. {self.stats['transformed_records']} records processed successfully.")
        if self.stats['invalid_records'] > 0:
            logger.warning(f"Found {self.stats['invalid_records']} invalid records.")
                
        return transformed
    
    def _parse_timestamp(self, timestamp_str: str) -> datetime:
        """Parse timestamp string to datetime object."""
        if not timestamp_str:
            raise ValueError("Timestamp is required")
            
        if isinstance(timestamp_str, datetime):
            return timestamp_str
            
        # Try multiple date formats
        for fmt in ('%Y-%m-%d %H:%M:%S', '%Y-%m-%d', '%d/%m/%Y %H:%M', '%Y%m%d'):
            try:
                return datetime.strptime(str(timestamp_str).strip(), fmt)
            except ValueError:
                continue
                
        raise ValueError(f"Could not parse timestamp: {timestamp_str}")
    
    def _validate_consumption(self, value: Any) -> float:
        """Validate and convert consumption value."""
        try:
            consumption = float(value)
            if not (self.min_consumption <= consumption <= self.max_consumption):
                raise ValueError(
                    f"Consumption {consumption} is outside valid range "
                    f"[{self.min_consumption}, {self.max_consumption}]"
                )
            return consumption
        except (TypeError, ValueError) as e:
            raise ValueError(f"Invalid consumption value: {value}") from e
    
    def _parse_float(self, value: Any, default: float = 0.0) -> float:
        """Safely parse a float value with a default."""
        try:
            return float(value) if value is not None else default
        except (TypeError, ValueError):
            return default
    
    def _calculate_quality_score(self, row: Dict[str, Any]) -> float:
        """Calculate a data quality score for the row (0-1)."""
        score = 1.0
        
        # Penalize missing values
        for field in ['timestamp', 'consumption_mwh', 'region']:
            if field not in row or row[field] is None:
                score -= 0.2
        
        return max(0.0, min(1.0, score))
    
    def _add_derived_features(self, row: Dict[str, Any]) -> Dict[str, Any]:
        """Add derived features to the row."""
        timestamp = row['timestamp']
        
        return {
            'hour_of_day': timestamp.hour,
            'day_of_week': timestamp.weekday(),
            'is_weekend': timestamp.weekday() >= 5,
            'month': timestamp.month,
            'year': timestamp.year,
            'season': self._get_season(timestamp.month)
        }
    
    @staticmethod
    def _get_season(month: int) -> str:
        """Get season based on month."""
        if 3 <= month <= 5:
            return 'spring'
        elif 6 <= month <= 8:
            return 'summer'
        elif 9 <= month <= 11:
            return 'autumn'
        else:
            return 'winter'
    
    def get_transformation_stats(self) -> Dict[str, Any]:
        """Get statistics about the transformation process."""
        return {
            **self.stats,
            'success_rate': self.stats['transformed_records'] / max(1, self.stats['total_records']),
            'error_rate': self.stats['invalid_records'] / max(1, self.stats['total_records'])
        }
