"""
Data Loaders

This package contains data loaders for the ETL pipeline.

Loaders are responsible for writing data to various target systems such as:
- Databases (PostgreSQL, MySQL, etc.)
- Data warehouses (BigQuery, Redshift, Snowflake, etc.)
- Data lakes (S3, Azure Data Lake, etc.)
- Message queues (Kafka, RabbitMQ, etc.)
- And more...

Modules:
    - base: Base loader class that all loaders should inherit from
    - database: Database loader for SQL databases
    - file: File-based loaders (CSV, JSON, Parquet, etc.)
    - api: API-based loaders
"""

# Import the database loader for easier access
from .database import DatabaseLoader

# Define what gets imported with 'from loaders import *'
__all__ = [
    'DatabaseLoader'
]