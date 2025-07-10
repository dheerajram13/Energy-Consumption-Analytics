# src/config/database.py - Fixed version
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, Session
import os
from dotenv import load_dotenv
import logging

# Load environment variables
load_dotenv()

logger = logging.getLogger(__name__)

# Database URL with fallback
DATABASE_URL = os.getenv(
    "DATABASE_URL", 
    "postgresql://postgres:postgres@localhost:5432/energy_analytics"
)

# Create engine with connection pooling
engine = create_engine(
    DATABASE_URL,
    pool_size=10,
    max_overflow=20,
    pool_pre_ping=True,
    echo=False  # Set to True for SQL debugging
)

# Create session factory
SessionLocal = sessionmaker(
    autocommit=False, 
    autoflush=False, 
    bind=engine
)

# Base class for models
Base = declarative_base()

def get_db_session() -> Session:
    """Dependency to get database session"""
    db = SessionLocal()
    try:
        yield db
    except Exception as e:
        logger.error(f"Database session error: {str(e)}")
        db.rollback()
        raise
    finally:
        db.close()

def get_db() -> Session:
    """Get database session (for non-dependency injection usage)"""
    return SessionLocal()

def init_db():
    """Initialize database tables"""
    try:
        # Import all models to ensure they're registered
        from ..models.energy_models import Base, User, PowerPlant, EnergyConsumption, Anomaly
        
        # Create all tables
        Base.metadata.create_all(bind=engine)
        logger.info("Database tables created successfully")
        
        # Create default admin user if not exists
        create_default_admin()
        
    except Exception as e:
        logger.error(f"Error initializing database: {str(e)}")
        raise

def create_default_admin():
    """Create default admin user if not exists"""
    try:
        from ..models.energy_models import User
        from ..auth.auth_utils import get_password_hash
        
        db = SessionLocal()
        
        # Check if admin user exists
        admin_user = db.query(User).filter(User.username == "admin").first()
        
        if not admin_user:
            # Create admin user
            admin = User(
                username="admin",
                email="admin@example.com",
                hashed_password=get_password_hash("admin"),
                full_name="System Administrator",
                is_active=True,
                is_superuser=True
            )
            
            db.add(admin)
            db.commit()
            logger.info("Default admin user created")
        
        db.close()
        
    except Exception as e:
        logger.error(f"Error creating default admin: {str(e)}")

def check_db_connection() -> bool:
    """Check if database connection is working"""
    try:
        # Try to execute a simple query
        with engine.connect() as connection:
            connection.execute("SELECT 1")
        return True
    except Exception as e:
        logger.error(f"Database connection failed: {str(e)}")
        return False