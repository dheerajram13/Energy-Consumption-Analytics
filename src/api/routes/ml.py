from fastapi import APIRouter, Depends, HTTPException, status
from fastapi.security import OAuth2PasswordBearer
from datetime import datetime, timedelta
from typing import Optional, Dict, Any
from sqlalchemy.orm import Session
from pydantic import BaseModel, Field

from ...ml.service import MLService
from ...config.database import get_db_session
from ...auth.auth_utils import get_current_active_user

router = APIRouter(
    prefix="/ml",
    tags=["machine_learning"],
    dependencies=[Depends(get_current_active_user)]
)

# Request/Response Models
class AnomalyDetectionRequest(BaseModel):
    start_date: datetime
    end_date: datetime
    region: Optional[str] = None
    model_params: Optional[Dict[str, Any]] = Field(default_factory=dict)
    fit_params: Optional[Dict[str, Any]] = Field(default_factory=dict)
    predict_params: Optional[Dict[str, Any]] = Field(default_factory=dict)

class ForecastRequest(BaseModel):
    start_date: datetime
    end_date: datetime
    n_periods: int = Field(24, ge=1, le=720)  # Max 30 days at hourly frequency
    region: Optional[str] = None
    model_type: str = Field("prophet", regex="^(prophet|exponential_smoothing|arima)$")
    model_params: Optional[Dict[str, Any]] = Field(default_factory=dict)
    fit_params: Optional[Dict[str, Any]] = Field(default_factory=dict)

class AnomalyStatsRequest(BaseModel):
    start_date: datetime
    end_date: datetime
    region: Optional[str] = None

@router.post("/detect-anomalies")
async def detect_anomalies(
    request: AnomalyDetectionRequest,
    db: Session = Depends(get_db_session)
):
    """
    Detect anomalies in energy consumption data.
    
    Args:
        request: Anomaly detection parameters
        db: Database session
        
    Returns:
        Dictionary with detected anomalies and model metrics
    """
    try:
        ml_service = MLService(db)
        result = ml_service.detect_anomalies(
            start_date=request.start_date,
            end_date=request.end_date,
            region=request.region,
            model_params=request.model_params,
            fit_params=request.fit_params,
            predict_params=request.predict_params
        )
        return result
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=str(e)
        )

@router.post("/forecast")
async def forecast_consumption(
    request: ForecastRequest,
    db: Session = Depends(get_db_session)
):
    """
    Forecast energy consumption.
    
    Args:
        request: Forecast parameters
        db: Database session
        
    Returns:
        Dictionary with forecast and model metrics
    """
    try:
        ml_service = MLService(db)
        result = ml_service.forecast_consumption(
            start_date=request.start_date,
            end_date=request.end_date,
            n_periods=request.n_periods,
            region=request.region,
            model_type=request.model_type,
            model_params=request.model_params,
            fit_params=request.fit_params
        )
        return result
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=str(e)
        )

@router.post("/anomaly-stats")
async def get_anomaly_stats(
    request: AnomalyStatsRequest,
    db: Session = Depends(get_db_session)
):
    """
    Get statistics about anomalies.
    
    Args:
        request: Stats parameters
        db: Database session
        
    Returns:
        Dictionary with anomaly statistics
    """
    try:
        ml_service = MLService(db)
        stats = ml_service.get_anomaly_stats(
            start_date=request.start_date,
            end_date=request.end_date,
            region=request.region
        )
        return stats
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=str(e)
        )
