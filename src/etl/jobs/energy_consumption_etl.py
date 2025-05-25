from pathlib import Path
from ..extractors.csv_extractor import CSVExtractor
from ..transformers.energy_transformer import EnergyConsumptionTransformer
from ..loaders.db_loader import EnergyConsumptionLoader
from ..base import ETLPipeline

def run_energy_consumption_etl(file_path: str, region: str = None):
    """
    Run ETL pipeline for energy consumption data.
    
    Args:
        file_path (str): Path to the CSV file
        region (str, optional): Region name. If not provided, will use region from data.
    """
    # Initialize ETL components
    extractor = CSVExtractor(file_path)
    transformer = EnergyConsumptionTransformer(region=region)
    loader = EnergyConsumptionLoader()
    
    # Create and run pipeline
    pipeline = ETLPipeline(extractor, transformer, loader)
    success = pipeline.run()
    
    return success

if __name__ == "__main__":
    # Example usage
    import os
    
    # Get the project root directory
    project_root = Path(__file__).parent.parent.parent.parent
    sample_data_path = project_root / "data" / "raw" / "sample_energy_data.csv"
    
    # Create sample data directory if it doesn't exist
    sample_data_path.parent.mkdir(parents=True, exist_ok=True)
    
    # Create sample data if it doesn't exist
    if not sample_data_path.exists():
        import pandas as pd
        from datetime import datetime, timedelta
        
        # Generate sample data
        dates = [datetime.now() - timedelta(hours=i) for i in range(100)]
        data = {
            'timestamp': [d.strftime('%Y-%m-%d %H:%M:%S') for d in dates],
            'consumption_mwh': [100 + i + (i % 24) * 10 for i in range(100)],
            'temperature': [20 + (i % 10) for i in range(100)],
            'is_holiday': [1 if i % 7 == 0 else 0 for i in range(100)]
        }
        
        df = pd.DataFrame(data)
        df.to_csv(sample_data_path, index=False)
    
    # Run ETL
    success = run_energy_consumption_etl(str(sample_data_path), "test_region")
    print(f"ETL {'succeeded' if success else 'failed'}")
