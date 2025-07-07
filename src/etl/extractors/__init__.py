"""
Data Extractors

This package contains data extractors for the ETL pipeline.

Extractors are responsible for retrieving data from various sources such as:
- Databases (PostgreSQL, MySQL, etc.)
- APIs (REST, GraphQL, etc.)
- Files (CSV, JSON, Excel, etc.)
- Message queues (Kafka, RabbitMQ, etc.)
- Web scraping
- And more...

Modules:
    - base: Base extractor class that all extractors should inherit from
    - api: API-based extractors
    - database: Database extractors
    - file: File-based extractors
    - web: Web scraping extractors
"""

# Import the base extractor for easier access
from .base import BaseExtractor

# Define what gets imported with 'from extractors import *'
__all__ = [
    'BaseExtractor'
]