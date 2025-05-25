# Energy Analytics Platform API Documentation

## Table of Contents
1. [Authentication](#authentication)
2. [Endpoints](#endpoints)
   - [Energy Consumption](#energy-consumption)
   - [Machine Learning](#machine-learning)
3. [Usage Examples](#usage-examples)

## Authentication

All API endpoints require authentication using JWT tokens. To authenticate:

1. First, obtain a token by making a POST request to `/api/token` with your username and password:

```http
POST /api/token
Content-Type: application/x-www-form-urlencoded

username=your_username&password=your_password
```

2. Include the token in subsequent requests using the `Authorization` header:
```
Authorization: Bearer your_token_here
```

## Endpoints

### Energy Consumption

#### Create Consumption Record
```http
POST /api/consumption/
Content-Type: application/json
Authorization: Bearer your_token_here

{
  "timestamp": "2023-01-01T00:00:00",
  "region": "north",
  "consumption_mwh": 150.5,
  "temperature": 22.5,
  "is_holiday": false
}
```

#### Get Consumption by ID
```http
GET /api/consumption/{consumption_id}
Authorization: Bearer your_token_here
```

#### Get Consumption Data
```http
GET /api/consumption/?start_date=2023-01-01&end_date=2023-01-31&region=north
Authorization: Bearer your_token_here
```

#### Update Consumption Record
```http
PUT /api/consumption/{consumption_id}
Content-Type: application/json
Authorization: Bearer your_token_here

{
  "consumption_mwh": 160.0,
  "temperature": 23.0
}
```

#### Delete Consumption Record
```http
DELETE /api/consumption/{consumption_id}
Authorization: Bearer your_token_here
```

#### Get Consumption Summary
```http
GET /api/consumption/summary?start_date=2023-01-01&end_date=2023-01-31&region=north
Authorization: Bearer your_token_here
```

### Machine Learning

#### Detect Anomalies
```http
POST /api/ml/detect-anomalies
Content-Type: application/json
Authorization: Bearer your_token_here

{
  "start_date": "2023-01-01T00:00:00",
  "end_date": "2023-01-31T23:59:59",
  "region": "north",
  "model_params": {
    "interval_width": 0.95
  },
  "fit_params": {
    "test_size": 0.2
  },
  "predict_params": {
    "threshold_std": 2.0
  }
}
```

#### Forecast Consumption
```http
POST /api/ml/forecast
Content-Type: application/json
Authorization: Bearer your_token_here

{
  "start_date": "2023-01-01T00:00:00",
  "end_date": "2023-01-31T23:59:59",
  "n_periods": 24,
  "region": "north",
  "model_type": "prophet",
  "model_params": {
    "daily_seasonality": true,
    "weekly_seasonality": true,
    "yearly_seasonality": false
  },
  "fit_params": {
    "test_size": 0.2
  }
}
```

#### Get Anomaly Statistics
```http
POST /api/ml/anomaly-stats
Content-Type: application/json
Authorization: Bearer your_token_here

{
  "start_date": "2023-01-01T00:00:00",
  "end_date": "2023-01-31T23:59:59",
  "region": "north"
}
```

## Usage Examples

### Python Example

```python
import requests
from datetime import datetime, timedelta

# Authentication
auth_url = "http://localhost:8000/api/token"
credentials = {
    "username": "admin",
    "password": "your_password_here"
}
response = requests.post(auth_url, data=credentials)
token = response.json()["access_token"]
headers = {"Authorization": f"Bearer {token}"}

# Get consumption data
end_date = datetime.now()
start_date = end_date - timedelta(days=30)

params = {
    "start_date": start_date.isoformat(),
    "end_date": end_date.isoformat(),
    "region": "north"
}

response = requests.get(
    "http://localhost:8000/api/consumption/",
    headers=headers,
    params=params
)

print(response.json())

# Detect anomalies
anomaly_data = {
    "start_date": start_date.isoformat(),
    "end_date": end_date.isoformat(),
    "region": "north"
}

response = requests.post(
    "http://localhost:8000/api/ml/detect-anomalies",
    headers=headers,
    json=anomaly_data
)

print(response.json())
```

### cURL Example

```bash
# Get token
curl -X POST "http://localhost:8000/api/token" \
  -H "Content-Type: application/x-www-form-urlencoded" \
  -d "username=admin&password=your_password_here"

# Get consumption data (replace TOKEN with actual token)
curl "http://localhost:8000/api/consumption/?start_date=2023-01-01&end_date=2023-01-31&region=north" \
  -H "Authorization: Bearer TOKEN"

# Detect anomalies (replace TOKEN with actual token)
curl -X POST "http://localhost:8000/api/ml/detect-anomalies" \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer TOKEN" \
  -d '{"start_date":"2023-01-01T00:00:00","end_date":"2023-01-31T23:59:59","region":"north"}'
```
