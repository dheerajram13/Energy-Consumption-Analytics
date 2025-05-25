import sys
import os
from pathlib import Path

# Add the project root to the Python path
project_root = Path(__file__).parent
sys.path.append(str(project_root))

from src.etl.jobs.energy_consumption_etl import run_energy_consumption_etl

def main():
    # Create data directory if it doesn't exist
    data_dir = project_root / "data" / "raw"
    data_dir.mkdir(parents=True, exist_ok=True)
    
    # Path for the sample data file
    sample_data_path = data_dir / "sample_energy_data.csv"
    
    print(f"Running ETL job with sample data at: {sample_data_path}")
    
    # Run the ETL job
    try:
        success = run_energy_consumption_etl(str(sample_data_path))
        if success:
            print("ETL job completed successfully!")
        else:
            print("ETL job completed with errors.")
    except Exception as e:
        print(f"Error running ETL job: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
