"""
ETL Base Classes

This module contains the base classes for the ETL pipeline.
"""
from abc import ABC, abstractmethod
from typing import List, Dict, Any, Optional, TypeVar, Generic, Type
from datetime import datetime
import logging
from dataclasses import dataclass, field
from enum import Enum, auto

# Set up logging
logger = logging.getLogger(__name__)

# Type variables for generic ETL components
T = TypeVar('T')
R = TypeVar('R')

class ETLStatus(Enum):
    """Status of an ETL operation."""
    PENDING = auto()
    RUNNING = auto()
    COMPLETED = auto()
    FAILED = auto()
    SKIPPED = auto()

@dataclass
class ETLStats:
    """Statistics for an ETL operation."""
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
    status: ETLStatus = ETLStatus.PENDING
    records_processed: int = 0
    records_skipped: int = 0
    records_failed: int = 0
    errors: List[Dict[str, Any]] = field(default_factory=list)
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    @property
    def duration(self) -> Optional[float]:
        """Get the duration of the ETL operation in seconds."""
        if self.start_time and self.end_time:
            return (self.end_time - self.start_time).total_seconds()
        return None
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert stats to a dictionary."""
        return {
            'status': self.status.name,
            'start_time': self.start_time.isoformat() if self.start_time else None,
            'end_time': self.end_time.isoformat() if self.end_time else None,
            'duration_seconds': self.duration,
            'records_processed': self.records_processed,
            'records_skipped': self.records_skipped,
            'records_failed': self.records_failed,
            'error_count': len(self.errors),
            'metadata': self.metadata
        }

class BaseExtractor(ABC, Generic[T]):
    """Base class for all extractors."""
    
    def __init__(self, name: Optional[str] = None):
        """Initialize the extractor.
        
        Args:
            name: Optional name for the extractor
        """
        self.name = name or self.__class__.__name__
        self.stats = ETLStats()
    
    @abstractmethod
    def extract(self, *args, **kwargs) -> T:
        """Extract data from a source.
        
        Returns:
            The extracted data in a format specific to the extractor
        """
        pass
    
    def get_stats(self) -> Dict[str, Any]:
        """Get statistics about the extraction process."""
        return self.stats.to_dict()


class BaseTransformer(ABC, Generic[T, R]):
    """Base class for all transformers."""
    
    def __init__(self, name: Optional[str] = None):
        """Initialize the transformer.
        
        Args:
            name: Optional name for the transformer
        """
        self.name = name or self.__class__.__name__
        self.stats = ETLStats()
    
    @abstractmethod
    def transform(self, data: T) -> R:
        """Transform the input data.
        
        Args:
            data: Input data to transform
            
        Returns:
            Transformed data
        """
        pass
    
    def get_stats(self) -> Dict[str, Any]:
        """Get statistics about the transformation process."""
        return self.stats.to_dict()


class BaseLoader(ABC, Generic[T]):
    """Base class for all loaders."""
    
    def __init__(self, name: Optional[str] = None):
        """Initialize the loader.
        
        Args:
            name: Optional name for the loader
        """
        self.name = name or self.__class__.__name__
        self.stats = ETLStats()
    
    @abstractmethod
    def load(self, data: T) -> bool:
        """Load the data to a destination.
        
        Args:
            data: Data to load
            
        Returns:
            True if the load was successful, False otherwise
        """
        pass
    
    def get_stats(self) -> Dict[str, Any]:
        """Get statistics about the load process."""
        return self.stats.to_dict()


class ETLPipeline(Generic[T, R]):
    """Generic ETL pipeline that orchestrates the extract, transform, and load process."""
    
    def __init__(
        self,
        extractor: BaseExtractor[T],
        transformer: BaseTransformer[T, R],
        loader: BaseLoader[R],
        name: Optional[str] = None
    ):
        """Initialize the ETL pipeline.
        
        Args:
            extractor: Extractor instance
            transformer: Transformer instance
            loader: Loader instance
            name: Optional name for the pipeline
        """
        self.extractor = extractor
        self.transformer = transformer
        self.loader = loader
        self.name = name or f"{extractor.name}_{transformer.name}_{loader.name}"
        self.stats = ETLStats()
    
    def run(self, *args, **kwargs) -> bool:
        """Run the ETL pipeline.
        
        Returns:
            True if the pipeline completed successfully, False otherwise
        """
        self.stats.status = ETLStatus.RUNNING
        self.stats.start_time = datetime.utcnow()
        
        try:
            # Extract
            logger.info(f"[{self.name}] Starting extraction...")
            extracted_data = self.extractor.extract(*args, **kwargs)
            
            # Transform
            logger.info(f"[{self.name}] Starting transformation...")
            transformed_data = self.transformer.transform(extracted_data)
            
            # Load
            logger.info(f"[{self.name}] Starting load...")
            result = self.loader.load(transformed_data)
            
            # Update stats
            self.stats.status = ETLStatus.COMPLETED if result else ETLStatus.FAILED
            self.stats.end_time = datetime.utcnow()
            
            if result:
                logger.info(f"[{self.name}] ETL completed successfully in {self.stats.duration:.2f} seconds")
            else:
                logger.error(f"[{self.name}] ETL failed after {self.stats.duration:.2f} seconds")
            
            return result
            
        except Exception as e:
            self.stats.status = ETLStatus.FAILED
            self.stats.end_time = datetime.utcnow()
            self.stats.errors.append({
                'error': str(e),
                'type': type(e).__name__,
                'timestamp': datetime.utcnow().isoformat()
            })
            
            logger.exception(f"[{self.name}] ETL failed after {self.stats.duration:.2f} seconds")
            raise
    
    def get_stats(self) -> Dict[str, Any]:
        """Get statistics about the ETL process."""
        stats = self.stats.to_dict()
        stats.update({
            'extractor': self.extractor.get_stats(),
            'transformer': self.transformer.get_stats(),
            'loader': self.loader.get_stats(),
            'name': self.name
        })
        return stats


class ETLPipelineBuilder(Generic[T, R]):
    """Builder for creating ETLPipeline instances."""
    
    def __init__(self):
        self._extractor = None
        self._transformer = None
        self._loader = None
        self._name = None
    
    def with_extractor(self, extractor: BaseExtractor[T]) -> 'ETLPipelineBuilder[T, R]':
        """Set the extractor."""
        self._extractor = extractor
        return self
    
    def with_transformer(self, transformer: BaseTransformer[T, R]) -> 'ETLPipelineBuilder[T, R]':
        """Set the transformer."""
        self._transformer = transformer
        return self
    
    def with_loader(self, loader: BaseLoader[R]) -> 'ETLPipelineBuilder[T, R]':
        """Set the loader."""
        self._loader = loader
        return self
    
    def with_name(self, name: str) -> 'ETLPipelineBuilder[T, R]':
        """Set the pipeline name."""
        self._name = name
        return self
    
    def build(self) -> ETLPipeline[T, R]:
        """Build the ETL pipeline."""
        if not all([self._extractor, self._transformer, self._loader]):
            raise ValueError("Extractor, transformer, and loader must be set")
            
        return ETLPipeline(
            extractor=self._extractor,
            transformer=self._transformer,
            loader=self._loader,
            name=self._name
        )
