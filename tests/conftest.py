# tests/conftest.py - Test configuration and fixtures
import pytest
import tempfile
import os
from datetime import datetime, timedelta
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from fastapi.testclient import TestClient
import pandas as pd
import numpy as np

from src.models.energy_models import Base, User, EnergyConsumption
from src.config.database import get_db_session
from src.main import app
from src.auth.auth_utils import get_password_hash, create_access_token

# Test database URL
TEST_DATABASE_URL = "sqlite:///./test.db"

@pytest.fixture(scope="session")
def test_engine():
    """Create test database engine"""
    engine = create_engine(TEST_DATABASE_URL, connect_args={"check_same_thread": False})
    Base.metadata.create_all(bind=engine)
    yield engine
    Base.metadata.drop_all(bind=engine)

@pytest.fixture(scope="function")
def test_db(test_engine):
    """Create test database session"""
    TestingSessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=test_engine)
    session = TestingSessionLocal()
    
    yield session
    
    session.close()
    # Clean up data after each test
    for table in reversed(Base.metadata.sorted_tables):
        test_engine.execute(table.delete())

@pytest.fixture
def test_user(test_db):
    """Create a test user"""
    user = User(
        username="testuser",
        email="test@example.com",
        hashed_password=get_password_hash("testpassword"),
        full_name="Test User",
        is_active=True,
        is_superuser=False
    )
    test_db.add(user)
    test_db.commit()
    test_db.refresh(user)
    return user

@pytest.fixture
def test_admin_user(test_db):
    """Create a test admin user"""
    admin = User(
        username="admin",
        email="admin@example.com",
        hashed_password=get_password_hash("adminpassword"),
        full_name="Admin User",
        is_active=True,
        is_superuser=True
    )
    test_db.add(admin)
    test_db.commit()
    test_db.refresh(admin)
    return admin

@pytest.fixture
def test_token(test_user):
    """Create a test JWT token"""
    return create_access_token(data={"sub": test_user.username})

@pytest.fixture
def test_client(test_db):
    """Create test client with database override"""
    def override_get_db():
        yield test_db
    
    app.dependency_overrides[get_db_session] = override_get_db
    
    with TestClient(app) as client:
        yield client
    
    app.dependency_overrides.clear()

@pytest.fixture
def sample_energy_data():
    """Generate sample energy consumption data"""
    data = []
    start_date = datetime(2023, 1, 1)
    
    for i in range(100):
        timestamp = start_date + timedelta(hours=i)
        data.append({
            'timestamp': timestamp,
            'region': 'test_region',
            'consumption_mwh': 100 + 50 * np.sin(i / 24 * 2 * np.pi) + np.random.normal(0, 5),
            'temperature': 20 + 10 * np.sin(i / 24 * 2 * np.pi) + np.random.normal(0, 2),
            'is_holiday': i % 168 == 0  # Every week
        })
    
    return pd.DataFrame(data)

@pytest.fixture
def sample_csv_file(sample_energy_data):
    """Create a temporary CSV file with sample data"""
    with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
        sample_energy_data.to_csv(f.name, index=False)
        yield f.name
    
    # Clean up
    os.unlink(f.name)
