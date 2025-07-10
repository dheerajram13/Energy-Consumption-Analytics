from fastapi import FastAPI, HTTPException, Depends, Query, Body, Path, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.security import OAuth2PasswordRequestForm
from datetime import datetime, timedelta
from typing import List, Optional, Dict, Any
from sqlalchemy.orm import Session
from pydantic import BaseModel, Field
import pandas as pd

# Import models and database
from .models.energy_models import EnergyConsumption, PowerPlant, Anomaly, User
from .config.database import get_db_session, init_db
from .auth.auth_utils import (
    get_password_hash,
    get_current_active_user,
    get_current_active_admin,
    authenticate_user,
    create_access_token,
    ACCESS_TOKEN_EXPIRE_MINUTES
)

# Import routers
from .api.routes import auth as auth_routes
from .api.routes import ml as ml_routes
from .api.routes import powerbi as powerbi_routes

# Pydantic models for request/response
class EnergyConsumptionCreate(BaseModel):
    timestamp: datetime
    region: str
    consumption_mwh: float = Field(..., gt=0, description="Energy consumption in MWh")
    temperature: Optional[float] = None
    is_holiday: bool = False

class EnergyConsumptionUpdate(BaseModel):
    timestamp: Optional[datetime] = None
    region: Optional[str] = None
    consumption_mwh: Optional[float] = Field(None, gt=0, description="Energy consumption in MWh")
    temperature: Optional[float] = None
    is_holiday: Optional[bool] = None

class EnergyConsumptionResponse(EnergyConsumptionCreate):
    id: int
    created_at: datetime

    class Config:
        orm_mode = True

# Initialize database tables
init_db()

app = FastAPI(
    title="Energy Analytics Platform API",
    description="API for Energy Consumption Analytics Platform",
    version="0.1.0",
    docs_url="/api/docs",
    redoc_url="/api/redoc",
    openapi_url="/api/openapi.json"
)

# Include routers
app.include_router(auth_routes.router, prefix="/api/auth", tags=["auth"])
app.include_router(ml_routes.router, prefix="/api/ml", tags=["ml"])
app.include_router(powerbi_routes.router, prefix="/api", tags=["powerbi"])

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
    expose_headers=["Content-Disposition"]
)

# API Endpoints
@app.get("/api/")
async def read_root():
    return {"message": "Welcome to Energy Analytics Platform API"}

@app.get("/api/health")
async def health_check():
    return {"status": "healthy"}

# Helper function to get consumption record by ID
def get_consumption_record(consumption_id: int, db: Session):
    db_consumption = db.query(EnergyConsumption).filter(EnergyConsumption.id == consumption_id).first()
    if db_consumption is None:
        raise HTTPException(status_code=404, detail="Consumption record not found")
    return db_consumption

# Energy Consumption Endpoints
@app.post("/api/consumption/", response_model=EnergyConsumptionResponse, status_code=status.HTTP_201_CREATED)
async def create_consumption(
    consumption: EnergyConsumptionCreate,
    db: Session = Depends(get_db_session)
):
    """
    Create a new energy consumption record
    """
    try:
        db_consumption = EnergyConsumption(**consumption.dict())
        db.add(db_consumption)
        db.commit()
        db.refresh(db_consumption)
        return db_consumption
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=400, detail=str(e))

@app.get("/api/consumption/{consumption_id}", response_model=EnergyConsumptionResponse)
async def get_consumption_by_id(
    consumption_id: int = Path(..., title="The ID of the consumption record", ge=1),
    db: Session = Depends(get_db_session)
):
    """
    Get a specific energy consumption record by ID
    """
    return get_consumption_record(consumption_id, db)

@app.get("/api/consumption/", response_model=List[Dict[str, Any]])
async def get_consumption(
    start_date: datetime = Query(..., description="Start date (YYYY-MM-DD)"),
    end_date: datetime = Query(..., description="End date (YYYY-MM-DD)"),
    region: Optional[str] = None,
    db: Session = Depends(get_db_session)
):
    """
    Get energy consumption data for a date range and optional region
    """
    try:
        query = db.query(EnergyConsumption).filter(
            EnergyConsumption.timestamp >= start_date,
            EnergyConsumption.timestamp <= end_date
        )
        
        if region:
            query = query.filter(EnergyConsumption.region == region)
            
        records = query.all()
        
        return [
            {
                "id": r.id,
                "timestamp": r.timestamp,
                "region": r.region,
                "consumption_mwh": r.consumption_mwh,
                "temperature": r.temperature,
                "is_holiday": r.is_holiday,
                "created_at": r.created_at
            } for r in records
        ]
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.put("/api/consumption/{consumption_id}", response_model=EnergyConsumptionResponse)
async def update_consumption(
    consumption_id: int,
    consumption_update: EnergyConsumptionUpdate,
    db: Session = Depends(get_db_session)
):
    """
    Update an existing energy consumption record
    """
    try:
        db_consumption = get_consumption_record(consumption_id, db)
        
        # Update fields from the request
        update_data = consumption_update.dict(exclude_unset=True)
        for field, value in update_data.items():
            setattr(db_consumption, field, value)
            
        db.commit()
        db.refresh(db_consumption)
        return db_consumption
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=400, detail=str(e))

@app.delete("/api/consumption/{consumption_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_consumption(
    consumption_id: int,
    db: Session = Depends(get_db_session)
):
    """
    Delete an energy consumption record
    """
    try:
        db_consumption = get_consumption_record(consumption_id, db)
        db.delete(db_consumption)
        db.commit()
        return None
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=400, detail=str(e))

@app.get("/api/consumption/summary")
async def get_consumption_summary(
    start_date: datetime = Query(..., description="Start date (YYYY-MM-DD)"),
    end_date: datetime = Query(..., description="End date (YYYY-MM-DD)"),
    region: Optional[str] = None,
    db: Session = Depends(get_db_session)
):
    """
    Get summary statistics for energy consumption
    """
    try:
        query = db.query(
            EnergyConsumption.region,
            db.func.count(EnergyConsumption.id).label("count"),
            db.func.sum(EnergyConsumption.consumption_mwh).label("total_consumption"),
            db.func.avg(EnergyConsumption.consumption_mwh).label("avg_consumption"),
            db.func.min(EnergyConsumption.consumption_mwh).label("min_consumption"),
            db.func.max(EnergyConsumption.consumption_mwh).label("max_consumption"),
            db.func.avg(EnergyConsumption.temperature).label("avg_temperature")
        ).filter(
            EnergyConsumption.timestamp >= start_date,
            EnergyConsumption.timestamp <= end_date
        )
        
        if region:
            query = query.filter(EnergyConsumption.region == region)
            
        results = query.group_by(EnergyConsumption.region).all()
        
        return [
            {
                "region": r.region,
                "count": r.count,
                "total_consumption": float(r.total_consumption) if r.total_consumption else 0,
                "avg_consumption": float(r.avg_consumption) if r.avg_consumption else 0,
                "min_consumption": float(r.min_consumption) if r.min_consumption else 0,
                "max_consumption": float(r.max_consumption) if r.max_consumption else 0,
                "avg_temperature": float(r.avg_temperature) if r.avg_temperature else None
            } for r in results
        ]
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
