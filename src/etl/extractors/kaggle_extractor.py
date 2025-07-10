import os
import pandas as pd
import kaggle
from typing import Optional, Dict, Any
import logging
from .base import BaseExtractor

logger = logging.getLogger(__name__)

class KaggleDataExtractor(BaseExtractor):
    """Extractor for Kaggle datasets"""
    
    def __init__(self, dataset_name: str, api_key: str = None, username: str = None):
        super().__init__()
        self.dataset_name = dataset_name
        self.api_key = api_key or os.getenv('KAGGLE_KEY')
        self.username = username or os.getenv('KAGGLE_USERNAME')
        
        # Configure Kaggle API
        os.environ['KAGGLE_USERNAME'] = self.username
        os.environ['KAGGLE_KEY'] = self.api_key
    
    def extract(self, output_path: str = './data/raw') -> pd.DataFrame:
        """Extract data from Kaggle dataset"""
        try:
            # Create output directory
            os.makedirs(output_path, exist_ok=True)
            
            # Download dataset
            kaggle.api.authenticate()
            kaggle.api.dataset_download_files(
                self.dataset_name,
                path=output_path,
                unzip=True
            )
            
            # Find the downloaded CSV file(s)
            csv_files = [f for f in os.listdir(output_path) if f.endswith('.csv')]
            
            if not csv_files:
                raise ValueError(f"No CSV files found in downloaded dataset: {self.dataset_name}")
            
            # Read the first CSV file (or combine multiple if needed)
            df = pd.read_csv(os.path.join(output_path, csv_files[0]))
            
            logger.info(f"Successfully extracted {len(df)} records from {self.dataset_name}")
            return df
            
        except Exception as e:
            logger.error(f"Failed to extract data from Kaggle: {str(e)}")
            raise
