from typing import List, Dict, Any
from datetime import datetime
from ..base import BaseTransformer

class EnergyConsumptionTransformer(BaseTransformer):
    def __init__(self, region: str = None):
        self.region = region
    
    def transform(self, data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        transformed = []
        
        for row in data:
            try:
                # Convert and validate data types
                transformed_row = {
                    'timestamp': datetime.strptime(row['timestamp'], '%Y-%m-%d %H:%M:%S'),
                    'consumption_mwh': float(row['consumption_mwh']),
                    'region': self.region or row.get('region', 'unknown'),
                    'temperature': float(row.get('temperature', 0)),
                    'is_holiday': int(row.get('is_holiday', 0))
                }
                transformed.append(transformed_row)
                
            except (ValueError, KeyError) as e:
                print(f"Error transforming row {row}: {str(e)}")
                continue
                
        return transformed
