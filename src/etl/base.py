from abc import ABC, abstractmethod
from typing import List, Dict, Any
from datetime import datetime

class BaseExtractor(ABC):
    @abstractmethod
    def extract(self, *args, **kwargs) -> List[Dict[str, Any]]:
        pass

class BaseTransformer(ABC):
    @abstractmethod
    def transform(self, data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        pass

class BaseLoader(ABC):
    @abstractmethod
    def load(self, data: List[Dict[str, Any]]) -> bool:
        pass

class ETLPipeline:
    def __init__(self, extractor: BaseExtractor, 
                 transformer: BaseTransformer, 
                 loader: BaseLoader):
        self.extractor = extractor
        self.transformer = transformer
        self.loader = loader
    
    def run(self, *args, **kwargs) -> bool:
        try:
            # Extract
            print("Extracting data...")
            extracted_data = self.extractor.extract(*args, **kwargs)
            
            # Transform
            print("Transforming data...")
            transformed_data = self.transformer.transform(extracted_data)
            
            # Load
            print("Loading data...")
            return self.loader.load(transformed_data)
            
        except Exception as e:
            print(f"ETL Pipeline failed: {str(e)}")
            raise
