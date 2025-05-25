import csv
from typing import List, Dict, Any
from pathlib import Path
from ..base import BaseExtractor

class CSVExtractor(BaseExtractor):
    def __init__(self, file_path: str):
        self.file_path = Path(file_path)
        
    def extract(self, *args, **kwargs) -> List[Dict[str, Any]]:
        if not self.file_path.exists():
            raise FileNotFoundError(f"File not found: {self.file_path}")
            
        data = []
        with open(self.file_path, 'r') as f:
            reader = csv.DictReader(f)
            for row in reader:
                data.append(dict(row))
                
        return data
