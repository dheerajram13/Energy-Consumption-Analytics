# src/config/logging.py - Logging configuration
import logging
import logging.config
import os
from typing import Dict, Any
from pathlib import Path
from .settings import settings

def get_logging_config() -> Dict[str, Any]:
    """Get logging configuration"""
    
    # Create logs directory if it doesn't exist
    logs_dir = Path("logs")
    logs_dir.mkdir(exist_ok=True)
    
    formatters = {
        "default": {
            "format": settings.LOG_FORMAT,
            "datefmt": "%Y-%m-%d %H:%M:%S",
        },
        "detailed": {
            "format": "%(asctime)s - %(name)s - %(levelname)s - %(module)s - %(funcName)s:%(lineno)d - %(message)s",
            "datefmt": "%Y-%m-%d %H:%M:%S",
        },
    }
    
    handlers = {
        "console": {
            "class": "logging.StreamHandler",
            "level": settings.LOG_LEVEL.value,
            "formatter": "default",
            "stream": "ext://sys.stdout",
        },
    }
    
    # Add file handler if specified
    if settings.LOG_FILE:
        log_file = Path(settings.LOG_FILE)
        log_file.parent.mkdir(parents=True, exist_ok=True)
        
        handlers["file"] = {
            "class": "logging.handlers.RotatingFileHandler",
            "level": settings.LOG_LEVEL.value,
            "formatter": "detailed",
            "filename": str(log_file.absolute()),
            "maxBytes": 10 * 1024 * 1024,  # 10MB
            "backupCount": 5,
            "encoding": "utf8",
        }
    
    # Add error file handler for production
    if settings.is_production:
        error_log = logs_dir / "errors.log"
        handlers["error_file"] = {
            "class": "logging.handlers.RotatingFileHandler",
            "level": "ERROR",
            "formatter": "detailed",
            "filename": str(error_log.absolute()),
            "maxBytes": 10 * 1024 * 1024,  # 10MB
            "backupCount": 10,
            "encoding": "utf8",
        }
    
    loggers = {
        "": {  # Root logger
            "level": settings.LOG_LEVEL.value,
            "handlers": list(handlers.keys()),
            "propagate": False,
        },
        "uvicorn": {
            "level": "INFO",
            "handlers": ["console"],
            "propagate": False,
        },
        "sqlalchemy.engine": {
            "level": "WARNING" if settings.is_production else "INFO",
            "handlers": ["console"],
            "propagate": False,
        },
        "fastapi": {
            "level": "INFO",
            "handlers": ["console"],
            "propagate": False,
        },
    }
    
    return {
        "version": 1,
        "disable_existing_loggers": False,
        "formatters": formatters,
        "handlers": handlers,
        "loggers": loggers,
    }

def setup_logging():
    """Setup logging configuration"""
    config = get_logging_config()
    logging.config.dictConfig(config)
    
    # Set log level for all loggers
    logging.getLogger().setLevel(settings.LOG_LEVEL.value)
    
    # Set log level for common libraries
    logging.getLogger("uvicorn.access").handlers = []
    logging.getLogger("uvicorn.error").propagate = False
    logging.getLogger("uvicorn").handlers = []
    
    # Disable noisy loggers in production
    if settings.is_production:
        logging.getLogger("botocore").setLevel(logging.WARNING)
        logging.getLogger("urllib3").setLevel(logging.WARNING)
        logging.getLogger("matplotlib").setLevel(logging.WARNING)

# Initialize logging when module is imported
setup_logging()
