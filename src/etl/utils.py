"""
ETL Utilities

This module contains utility functions for the ETL pipeline.
"""
import os
import json
import logging
from typing import Any, Dict, List, Optional, Union
from datetime import datetime, date, time
from pathlib import Path
import gzip
import pickle
import hashlib
import pandas as pd
import numpy as np

logger = logging.getLogger(__name__)

def ensure_directory_exists(directory: Union[str, Path]) -> Path:
    """Ensure that a directory exists, creating it if necessary.
    
    Args:
        directory: Directory path
        
    Returns:
        Path object for the directory
    """
    path = Path(directory)
    path.mkdir(parents=True, exist_ok=True)
    return path

def save_json(data: Any, filepath: Union[str, Path], compress: bool = False) -> None:
    """Save data to a JSON file.
    
    Args:
        data: Data to save (must be JSON-serializable)
        filepath: Path to the output file
        compress: Whether to compress the output with gzip
    """
    filepath = Path(filepath)
    ensure_directory_exists(filepath.parent)
    
    json_str = json.dumps(data, indent=2, default=_json_serializer)
    
    if compress:
        if not filepath.name.endswith('.gz'):
            filepath = filepath.with_suffix(filepath.suffix + '.gz')
        with gzip.open(filepath, 'wt', encoding='utf-8') as f:
            f.write(json_str)
    else:
        with open(filepath, 'w', encoding='utf-8') as f:
            f.write(json_str)
    
    logger.debug(f"Saved JSON data to {filepath}")

def load_json(filepath: Union[str, Path]) -> Any:
    """Load data from a JSON file.
    
    Args:
        filepath: Path to the JSON file (can be .json or .json.gz)
        
    Returns:
        The loaded data
    """
    filepath = Path(filepath)
    
    if filepath.suffix == '.gz' or filepath.suffixes[-1] == '.gz':
        with gzip.open(filepath, 'rt', encoding='utf-8') as f:
            return json.load(f)
    else:
        with open(filepath, 'r', encoding='utf-8') as f:
            return json.load(f)

def save_pickle(obj: Any, filepath: Union[str, Path], compress: bool = False) -> None:
    """Save a Python object to a pickle file.
    
    Args:
        obj: Object to save
        filepath: Path to the output file
        compress: Whether to compress the output with gzip
    """
    filepath = Path(filepath)
    ensure_directory_exists(filepath.parent)
    
    if compress:
        if not filepath.name.endswith('.pkl.gz'):
            filepath = filepath.with_suffix('.pkl.gz')
        with gzip.open(filepath, 'wb') as f:
            pickle.dump(obj, f, protocol=pickle.HIGHEST_PROTOCOL)
    else:
        with open(filepath, 'wb') as f:
            pickle.dump(obj, f, protocol=pickle.HIGHEST_PROTOCOL)
    
    logger.debug(f"Saved pickle to {filepath}")

def load_pickle(filepath: Union[str, Path]) -> Any:
    """Load a Python object from a pickle file.
    
    Args:
        filepath: Path to the pickle file (can be .pkl or .pkl.gz)
        
    Returns:
        The loaded object
    """
    filepath = Path(filepath)
    
    if filepath.suffix == '.gz' or filepath.suffixes[-1] == '.gz':
        with gzip.open(filepath, 'rb') as f:
            return pickle.load(f)
    else:
        with open(filepath, 'rb') as f:
            return pickle.load(f)

def calculate_md5(filepath: Union[str, Path], chunk_size: int = 8192) -> str:
    """Calculate the MD5 hash of a file.
    
    Args:
        filepath: Path to the file
        chunk_size: Size of chunks to read at a time
        
    Returns:
        The MD5 hash of the file
    """
    filepath = Path(filepath)
    hash_md5 = hashlib.md5()
    
    with open(filepath, 'rb') as f:
        for chunk in iter(lambda: f.read(chunk_size), b''):
            hash_md5.update(chunk)
    
    return hash_md5.hexdigest()

def _json_serializer(obj: Any) -> Any:
    """JSON serializer for objects not serializable by default."""
    if isinstance(obj, (datetime, date, time)):
        return obj.isoformat()
    elif isinstance(obj, np.integer):
        return int(obj)
    elif isinstance(obj, np.floating):
        return float(obj)
    elif isinstance(obj, np.ndarray):
        return obj.tolist()
    elif hasattr(obj, 'to_dict'):
        return obj.to_dict()
    elif hasattr(obj, 'dict'):
        return obj.dict()
    elif isinstance(obj, bytes):
        return obj.decode('utf-8', 'ignore')
    elif isinstance(obj, (set, frozenset)):
        return list(obj)
    elif pd.isna(obj):
        return None
    
    raise TypeError(f"Type {type(obj)} not serializable")

def chunk_list(lst: List[Any], chunk_size: int) -> List[List[Any]]:
    """Split a list into chunks of the specified size.
    
    Args:
        lst: List to split
        chunk_size: Maximum size of each chunk
        
    Returns:
        List of chunks
    """
    return [lst[i:i + chunk_size] for i in range(0, len(lst), chunk_size)]

def format_bytes(size: float) -> str:
    """Format a size in bytes as a human-readable string.
    
    Args:
        size: Size in bytes
        
    Returns:
        Formatted string (e.g., '1.2 MB')
    """
    for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
        if size < 1024.0:
            return f"{size:.1f} {unit}"
        size /= 1024.0
    return f"{size:.1f} PB"

def get_file_size(filepath: Union[str, Path]) -> str:
    """Get the size of a file as a human-readable string.
    
    Args:
        filepath: Path to the file
        
    Returns:
        Formatted file size (e.g., '1.2 MB')
    """
    filepath = Path(filepath)
    if not filepath.exists():
        raise FileNotFoundError(f"File not found: {filepath}")
    
    return format_bytes(filepath.stat().st_size)
