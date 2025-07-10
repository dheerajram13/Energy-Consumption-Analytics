# src/api/routes/energy.py - Complete energy consumption routes
from fastapi import APIRouter, Depends, HTTPException, status, Query, BackgroundTasks
from sqlalchemy.orm import Session
from sqlalchemy import func, and_
from typing import List, Optional, Dict, Any
from datetime import datetime, timedelta
from pydantic import BaseModel, Field

from ...models.energy_models import EnergyConsumption, User, Anomaly
from ...config.database import get_db_session
from ...auth.auth_utils import get_current_active_user

router = APIRouter(prefix="/energy", tags=["energy"])

# Pydantic models
class EnergyConsumptionCreate(BaseModel):
    timestamp: datetime
    region: str
    consumption_mwh: float = Field(..., gt=0)
    temperature: Optional[float] = None
    is_holiday: bool = False

class EnergyConsumptionResponse(BaseModel):
    id: int
    timestamp: datetime
    region: str
    consumption_mwh: float
    temperature: Optional[float]
    is_holiday: bool
    created_at: datetime

    class Config:
        orm_mode = True

class EnergyStats(BaseModel):
    total_consumption: float
    avg_consumption: float
    min_consumption: float
    max_consumption: float
    record_count: int
    regions: List[str]

@router.post("/consumption/", response_model=EnergyConsumptionResponse)
async def create_consumption_record(
    consumption: EnergyConsumptionCreate,
    db: Session = Depends(get_db_session),
    current_user: User = Depends(get_current_active_user)
):
    """Create a new energy consumption record"""
    try:
        db_consumption = EnergyConsumption(
            **consumption.dict(),
            user_id=current_user.id
        )
        db.add(db_consumption)
        db.commit()
        db.refresh(db_consumption)
        return db_consumption
    except Exception as e:
        db.rollback()
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Error creating consumption record: {str(e)}"
        )

@router.get("/consumption/", response_model=List[EnergyConsumptionResponse])
async def get_consumption_records(
    start_date: Optional[datetime] = Query(None),
    end_date: Optional[datetime] = Query(None),
    region: Optional[str] = Query(None),
    limit: int = Query(100, le=1000),
    offset: int = Query(0, ge=0),
    db: Session = Depends(get_db_session),
    current_user: User = Depends(get_current_active_user)
):
    """Get energy consumption records with filtering"""
    
    query = db.query(EnergyConsumption)
    
    # Apply filters
    if start_date:
        query = query.filter(EnergyConsumption.timestamp >= start_date)
    if end_date:
        query = query.filter(EnergyConsumption.timestamp <= end_date)
    if region:
        query = query.filter(EnergyConsumption.region == region)
    
    # Add pagination
    query = query.offset(offset).limit(limit)
    
    # Order by timestamp descending
    query = query.order_by(EnergyConsumption.timestamp.desc())
    
    return query.all()

@router.get("/consumption/{consumption_id}", response_model=EnergyConsumptionResponse)
async def get_consumption_by_id(
    consumption_id: int,
    db: Session = Depends(get_db_session),
    current_user: User = Depends(get_current_active_user)
):
    """Get a specific consumption record by ID"""
    
    consumption = db.query(EnergyConsumption).filter(
        EnergyConsumption.id == consumption_id
    ).first()
    
    if not consumption:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Consumption record not found"
        )
    
    return consumption

@router.get("/stats/", response_model=EnergyStats)
async def get_energy_statistics(
    start_date: Optional[datetime] = Query(None),
    end_date: Optional[datetime] = Query(None),
    region: Optional[str] = Query(None),
    db: Session = Depends(get_db_session),
    current_user: User = Depends(get_current_active_user)
):
    """Get energy consumption statistics"""
    
    query = db.query(
        func.sum(EnergyConsumption.consumption_mwh).label('total'),
        func.avg(EnergyConsumption.consumption_mwh).label('avg'),
        func.min(EnergyConsumption.consumption_mwh).label('min'),
        func.max(EnergyConsumption.consumption_mwh).label('max'),
        func.count(EnergyConsumption.id).label('count')
    )
    
    # Apply filters
    if start_date:
        query = query.filter(EnergyConsumption.timestamp >= start_date)
    if end_date:
        query = query.filter(EnergyConsumption.timestamp <= end_date)
    if region:
        query = query.filter(EnergyConsumption.region == region)
    
    result = query.first()
    
    # Get unique regions
    regions_query = db.query(EnergyConsumption.region).distinct()
    if start_date:
        regions_query = regions_query.filter(EnergyConsumption.timestamp >= start_date)
    if end_date:
        regions_query = regions_query.filter(EnergyConsumption.timestamp <= end_date)
    
    regions = [r[0] for r in regions_query.all() if r[0]]
    
    return EnergyStats(
        total_consumption=float(result.total) if result.total else 0.0,
        avg_consumption=float(result.avg) if result.avg else 0.0,
        min_consumption=float(result.min) if result.min else 0.0,
        max_consumption=float(result.max) if result.max else 0.0,
        record_count=result.count or 0,
        regions=regions
    )

@router.get("/regions/")
async def get_regions(
    db: Session = Depends(get_db_session),
    current_user: User = Depends(get_current_active_user)
):
    """Get all available regions"""
    
    regions = db.query(EnergyConsumption.region).distinct().all()
    return [r[0] for r in regions if r[0]]

@router.delete("/consumption/{consumption_id}")
async def delete_consumption_record(
    consumption_id: int,
    db: Session = Depends(get_db_session),
    current_user: User = Depends(get_current_active_user)
):
    """Delete a consumption record"""
    
    consumption = db.query(EnergyConsumption).filter(
        EnergyConsumption.id == consumption_id
    ).first()
    
    if not consumption:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Consumption record not found"
        )
    
    # Check if user owns the record or is admin
    if consumption.user_id != current_user.id and not current_user.is_superuser:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Not authorized to delete this record"
        )
    
    db.delete(consumption)
    db.commit()
    
    return {"message": "Consumption record deleted successfully"}
