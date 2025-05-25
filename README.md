# Energy Consumption Analytics Platform

A comprehensive energy analytics platform that processes power plant data and smart meter time-series information, providing ML-based anomaly detection, forecasting, and interactive dashboards for data-driven energy decisions.

## 🚀 Features

- **ETL Pipelines**: Process 15+ GB of time-series data
- **ML Models**: Anomaly detection and forecasting using Darts library
- **RESTful API**: FastAPI-based endpoints for data access and ML predictions
- **Data Analysis**: Regional consumption pattern analysis and benchmarking
- **Database**: PostgreSQL for reliable data storage
- **Containerized**: Easy deployment with Docker
- **Authentication**: JWT-based secure authentication
- **Anomaly Detection**: Identify unusual consumption patterns
- **Forecasting**: Predict future energy consumption

## 🛠️ Tech Stack

- **Backend**: Python 3.9+, FastAPI, SQLAlchemy
- **Database**: PostgreSQL
- **ETL**: Custom ETL framework, Pandas
- **ML**: Darts, Scikit-learn, pmdarima, Prophet
- **Containerization**: Docker, Docker Compose
- **Workflow**: Apache Airflow
- **Authentication**: JWT, OAuth2 with Password (and hashing)

## 🚀 Quick Start

### Prerequisites

- Python 3.9+
- PostgreSQL
- Docker and Docker Compose (for containerized deployment)

### Local Development

1. **Clone the repository**
   ```bash
   git clone https://github.com/yourusername/Energy-Consumption-Analytics.git
   cd Energy-Consumption-Analytics
   ```

2. **Set up a virtual environment**
   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```

3. **Install dependencies**
   ```bash
   pip install -r requirements.txt
   ```

4. **Set up environment variables**
   Copy `.env.example` to `.env` and update the database connection details:
   ```env
   # Database
   DATABASE_URL=postgresql://user:password@localhost:5432/energy_db
   
   # JWT
   SECRET_KEY=your-secret-key-here
   ALGORITHM=HS256
   ACCESS_TOKEN_EXPIRE_MINUTES=30
   ```

5. **Initialize the database**
   ```bash
   python -m src.config.database
   ```

6. **Run the API server**
   ```bash
   uvicorn src.main:app --reload
   ```

7. **Access the API documentation**
   - Swagger UI: http://localhost:8000/api/docs
   - ReDoc: http://localhost:8000/api/redoc

## 📚 API Documentation

Detailed API documentation is available in [API_DOCS.md](API_DOCS.md).

## 🤖 ML Features

### Anomaly Detection
- Detect unusual energy consumption patterns
- Configurable threshold for anomaly sensitivity
- Historical anomaly statistics and analysis

### Consumption Forecasting
- Time-series forecasting using multiple models (Prophet, ARIMA, Exponential Smoothing)
- Model evaluation metrics (MAE, MSE, RMSE, MAPE)
- Cross-validation support

## 🧪 Testing

Run the test suite with:

```bash
pytest tests/
```

## 🐳 Docker Deployment

1. Build and start the containers:
   ```bash
   docker-compose up -d --build
   ```

2. Access the application:
   - API: http://localhost:8000
   - Adminer (Database UI): http://localhost:8080

## 📊 Data Model

### Energy Consumption
- `id`: Primary key
- `timestamp`: Timestamp of the reading
- `region`: Geographic region
- `consumption_mwh`: Energy consumption in MWh
- `temperature`: Temperature at the time of reading (optional)
- `is_holiday`: Whether it was a holiday (boolean)
- `created_at`: Timestamp when the record was created

### Anomalies
- `id`: Primary key
- `timestamp`: Timestamp of the anomaly
- `region`: Geographic region
- `actual_value`: Actual consumption value
- `predicted_value`: Predicted consumption value
- `anomaly_score`: Severity of the anomaly
- `is_confirmed`: Whether the anomaly was confirmed by a user

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/AmazingFeature`)
3. Commit your changes (`git commit -m 'Add some AmazingFeature'`)
4. Push to the branch (`git push origin feature/AmazingFeature`)
5. Open a Pull Request

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## 🙏 Acknowledgments

- [FastAPI](https://fastapi.tiangolo.com/) - The web framework used
- [Darts](https://unit8co.github.io/darts/) - Time series forecasting library
- [SQLAlchemy](https://www.sqlalchemy.org/) - The Database Toolkit for Python
- [pandas](https://pandas.pydata.org/) - Data analysis and manipulation tool
