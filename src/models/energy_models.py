from sqlalchemy import Column, Integer, Float, DateTime, String, ForeignKey, Boolean
from sqlalchemy.orm import relationship
from sqlalchemy.ext.declarative import declarative_base
from datetime import datetime

Base = declarative_base()

class User(Base):
    __tablename__ = "users"

    id = Column(Integer, primary_key=True, index=True)
    username = Column(String, unique=True, index=True)
    email = Column(String, unique=True, index=True, nullable=True)
    hashed_password = Column(String)
    full_name = Column(String, nullable=True)
    is_active = Column(Boolean, default=True)
    is_superuser = Column(Boolean, default=False)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    # Relationships
    consumption_records = relationship("EnergyConsumption", back_populates="user")

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
    is_holiday = Column(Boolean, default=False)
    created_at = Column(DateTime, default=datetime.utcnow)
    
    # Relationships
    user_id = Column(Integer, ForeignKey("users.id"), nullable=True)
    user = relationship("User", back_populates="consumption_records")

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
