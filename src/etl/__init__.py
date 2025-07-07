"""
ETL Package

This package contains the ETL (Extract, Transform, Load) components for the
energy consumption analytics platform.

Modules:
    - base: Base classes for ETL components
    - config: Configuration settings for the ETL pipeline
    - extractors: Data extraction components
    - loaders: Data loading components
    - transformers: Data transformation components
    - utils: Utility functions for ETL
"""

# Version of the etl package
__version__ = "0.1.0"

# Import key components for easier access
from .base import (
    BaseExtractor,
    BaseTransformer,
    BaseLoader,
    ETLPipeline,
    ETLPipelineBuilder,
    ETLStatus,
    ETLStats
)

# Import commonly used components
from .extractors.base import BaseExtractor as Extractor
from .transformers.energy_transformer import EnergyConsumptionTransformer as Transformer
from .loaders.database import DatabaseLoader as Loader

# Import configuration
from .config import get_etl_config

# Create a default configuration instance
config = get_etl_config()

# Define what gets imported with 'from etl import *'
__all__ = [
    'BaseExtractor',
    'BaseTransformer',
    'BaseLoader',
    'ETLPipeline',
    'ETLPipelineBuilder',
    'ETLStatus',
    'ETLStats',
    'Extractor',
    'Transformer',
    'Loader',
    'config',
    'get_etl_config'
]
