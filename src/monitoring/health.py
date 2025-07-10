"""Health check and monitoring endpoints for the application."""
from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session
from sqlalchemy import text
from typing import Dict, Any, Optional
import psutil
import time
import os

from ..config.database import get_db_session
from .metrics import metrics

router = APIRouter(prefix="/health", tags=["health"])

@router.get("/")
async def health_check() -> Dict[str, Any]:
    """
    Basic health check endpoint.
    
    Returns:
        Dict with status and timestamp
    """
    return {
        "status": "healthy",
        "timestamp": time.time(),
        "service": "energy-analytics-api",
        "version": os.getenv("APP_VERSION", "dev")
    }

@router.get("/detailed")
async def detailed_health_check(
    db: Session = Depends(get_db_session)
) -> Dict[str, Any]:
    """
    Detailed health check with component status.
    
    Checks:
    - Database connectivity
    - System resources (CPU, memory, disk)
    - Service status
    
    Returns:
        Detailed health status with component checks
    """
    health_status = {
        "status": "healthy",
        "timestamp": time.time(),
        "checks": {}
    }
    
    # Database check
    try:
        db.execute(text("SELECT 1"))
        health_status["checks"]["database"] = {
            "status": "healthy",
            "connection": "ok"
        }
    except Exception as e:
        health_status["checks"]["database"] = {
            "status": "unhealthy", 
            "error": str(e)
        }
        health_status["status"] = "unhealthy"
    
    # System resources
    try:
        cpu_percent = psutil.cpu_percent(interval=1)
        memory = psutil.virtual_memory()
        disk = psutil.disk_usage('/')
        
        health_status["checks"]["system"] = {
            "status": "healthy",
            "cpu_percent": cpu_percent,
            "memory_percent": memory.percent,
            "disk_percent": disk.percent
        }
        
        # Alert if resources are too high
        if cpu_percent > 90 or memory.percent > 90 or disk.percent > 90:
            health_status["checks"]["system"]["status"] = "degraded"
            health_status["status"] = "degraded"
            
    except Exception as e:
        health_status["checks"]["system"] = {
            "status": "unhealthy",
            "error": str(e)
        }
        health_status["status"] = "unhealthy"
    
    return health_status

@router.get("/metrics")
async def get_metrics() -> Dict[str, Any]:
    """
    Get application metrics.
    
    Returns:
        Dictionary containing collected metrics
    """
    return metrics.get_metrics()

@router.get("/ready")
async def readiness_check(
    db: Session = Depends(get_db_session)
) -> Dict[str, str]:
    """
    Kubernetes readiness probe endpoint.
    
    Returns:
        Status indicating if the service is ready to receive traffic
    """
    try:
        # Check if database is ready
        db.execute(text("SELECT 1"))
        return {"status": "ready"}
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=f"Service not ready: {str(e)}"
        )

@router.get("/live")
async def liveness_check() -> Dict[str, str]:
    """
    Kubernetes liveness probe endpoint.
    
    Returns:
        Status indicating if the service is alive
    """
    return {"status": "alive"}
