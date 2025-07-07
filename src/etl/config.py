"""
ETL Configuration

This module contains configuration settings for the ETL pipeline.
"""
import os
from typing import Dict, Any, Optional
from pydantic import validator, PostgresDsn
from pydantic_settings import BaseSettings

class ETLConfig(BaseSettings):
    """Configuration for the ETL pipeline."""
    
    # Database settings
    POSTGRES_USER: str = "postgres"
    POSTGRES_PASSWORD: str = "postgres"
    POSTGRES_SERVER: str = "db"
    POSTGRES_PORT: str = "5432"
    POSTGRES_DB: str = "energy_analytics"
    DATABASE_URL: Optional[PostgresDsn] = None
    
    # ETL settings
    BATCH_SIZE: int = 1000
    MAX_RETRIES: int = 3
    RETRY_DELAY: int = 5  # seconds
    
    # Data validation
    MIN_CONSUMPTION: float = 0.0
    MAX_CONSUMPTION: float = 100000.0
    
    # Data sources
    KAGGLE_DATASET: str = "unitednations/global-commodity-trade-statistics"
    EXTERNAL_API_URL: str = "https://api.energydata.example.com"
    
    # Logging
    LOG_LEVEL: str = "INFO"
    LOG_FORMAT: str = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    
    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"
        case_sensitive = True
    
    @validator("DATABASE_URL", pre=True)
    def assemble_db_connection(cls, v: Optional[str], values: Dict[str, Any]) -> Any:
        """Assemble the database connection string if not provided."""
        if isinstance(v, str):
            return v
            
        return f"postgresql://{values.get('POSTGRES_USER')}:{values.get('POSTGRES_PASSWORD')}@{values.get('POSTGRES_SERVER')}:{values.get('POSTGRES_PORT')}/{values.get('POSTGRES_DB')}"

# Create a singleton instance of the config
config = ETLConfig()

def get_etl_config() -> ETLConfig:
    """Get the ETL configuration."""
    return config
