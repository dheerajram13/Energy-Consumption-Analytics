from sqlalchemy import Column, Integer, Float, DateTime, String, ForeignKey
from sqlalchemy.ext.declarative import declarative_base
from datetime import datetime

Base = declarative_base()

class PowerPlant(Base):
    __tablename__ = 'power_plants'
    
    id = Column(Integer, primary_key=True, index=True)
    name = Column(String, unique=True, index=True)
    region = Column(String, index=True)
    capacity_mw = Column(Float)
    fuel_type = Column(String)
    created_at = Column(DateTime, default=datetime.utcnow)

class EnergyConsumption(Base):
    __tablename__ = 'energy_consumption'
    
    id = Column(Integer, primary_key=True, index=True)
    timestamp = Column(DateTime, index=True)
    region = Column(String, index=True)
    consumption_mwh = Column(Float)
    temperature = Column(Float, nullable=True)
    is_holiday = Column(Integer, default=0)
    created_at = Column(DateTime, default=datetime.utcnow)

class Anomaly(Base):
    __tablename__ = 'anomalies'
    
    id = Column(Integer, primary_key=True, index=True)
    timestamp = Column(DateTime, index=True)
    region = Column(String, index=True)
    actual_value = Column(Float)
    predicted_value = Column(Float)
    anomaly_score = Column(Float)
    is_confirmed = Column(Integer, default=0)
    created_at = Column(DateTime, default=datetime.utcnow)
