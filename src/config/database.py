from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from ..models.energy_models import Base
import os
from dotenv import load_dotenv

load_dotenv()

DATABASE_URL = os.getenv(
    "DATABASE_URL", 
    "postgresql://postgres:postgres@localhost:5432/energy_analytics"
)

def get_db():
    engine = create_engine(DATABASE_URL)
    return engine

def init_db():
    engine = get_db()
    Base.metadata.create_all(bind=engine)
    return engine

# Create session factory
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=get_db())

def get_db_session():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
