# tests/test_api.py - API tests
import pytest
from fastapi import status
from datetime import datetime
from sqlalchemy.orm import Session
from src.models.energy_models import EnergyConsumption

def test_health_check(test_client):
    """Test health check endpoint"""
    response = test_client.get("/api/health")
    assert response.status_code == 200
    assert response.json() == {"status": "healthy"}

def test_create_user(test_client):
    """Test user registration"""
    user_data = {
        "username": "newuser",
        "email": "newuser@example.com",
        "password": "newpassword",
        "full_name": "New User"
    }
    
    response = test_client.post("/api/auth/register", json=user_data)
    assert response.status_code == status.HTTP_201_CREATED
    assert response.json()["username"] == "newuser"

def test_login(test_client, test_user):
    """Test user login"""
    login_data = {
        "username": "testuser",
        "password": "testpassword"
    }
    
    response = test_client.post("/api/auth/token", data=login_data)
    assert response.status_code == 200
    assert "access_token" in response.json()

def test_create_consumption_record(test_client, test_token):
    """Test creating energy consumption record"""
    headers = {"Authorization": f"Bearer {test_token}"}
    
    consumption_data = {
        "timestamp": "2023-01-01T12:00:00",
        "region": "test_region",
        "consumption_mwh": 150.5,
        "temperature": 25.0,
        "is_holiday": False
    }
    
    response = test_client.post(
        "/api/energy/consumption/",
        json=consumption_data,
        headers=headers
    )
    assert response.status_code == status.HTTP_201_CREATED
    assert response.json()["consumption_mwh"] == 150.5

def test_get_consumption_records(test_client, test_token, test_db, test_user):
    """Test getting consumption records"""
    # Create test data
    consumption = EnergyConsumption(
        timestamp=datetime(2023, 1, 1, 12, 0, 0),
        region="test_region",
        consumption_mwh=150.5,
        temperature=25.0,
        is_holiday=False,
        user_id=test_user.id
    )
    test_db.add(consumption)
    test_db.commit()
    
    headers = {"Authorization": f"Bearer {test_token}"}
    response = test_client.get("/api/energy/consumption/", headers=headers)
    
    assert response.status_code == 200
    assert len(response.json()) == 1
    assert response.json()[0]["consumption_mwh"] == 150.5
