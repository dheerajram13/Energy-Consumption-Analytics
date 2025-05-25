from typing import List, Dict, Any
from sqlalchemy.orm import Session
from sqlalchemy.exc import SQLAlchemyError
from ...models.energy_models import EnergyConsumption
from ...config.database import SessionLocal
from ..base import BaseLoader

class DatabaseLoader(BaseLoader):
    def __init__(self, model_class):
        self.model_class = model_class
        self.db = SessionLocal()
    
    def load(self, data: List[Dict[str, Any]]) -> bool:
        try:
            # Convert dicts to model instances
            instances = [self.model_class(**item) for item in data]
            
            # Bulk insert
            self.db.bulk_save_objects(instances)
            self.db.commit()
            return True
            
        except SQLAlchemyError as e:
            self.db.rollback()
            print(f"Database error: {str(e)}")
            return False
            
        except Exception as e:
            print(f"Error loading data: {str(e)}")
            return False
            
        finally:
            self.db.close()

class EnergyConsumptionLoader(DatabaseLoader):
    def __init__(self):
        super().__init__(EnergyConsumption)
