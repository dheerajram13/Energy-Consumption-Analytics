# Energy Consumption Analytics Platform

A comprehensive energy analytics platform that processes power plant data and smart meter time-series information, providing ML-based anomaly detection, forecasting, and interactive dashboards for data-driven energy decisions.

## 🚀 Features

- **ETL Pipelines**: Process 15+ GB of time-series data
- **ML Models**: Anomaly detection and forecasting using Darts library
- **RESTful API**: FastAPI-based endpoints for data access
- **Data Analysis**: Regional consumption pattern analysis and benchmarking
- **Database**: PostgreSQL for reliable data storage
- **Containerized**: Easy deployment with Docker

## 🛠️ Tech Stack

- **Backend**: Python 3.9+, FastAPI, SQLAlchemy
- **Database**: PostgreSQL
- **ETL**: Custom ETL framework, Pandas
- **ML**: Darts, Scikit-learn, pmdarima
- **Containerization**: Docker, Docker Compose
- **Workflow**: Apache Airflow

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
   ```bash
   cp .env.example .env
   ```

5. **Initialize the database**
   ```bash
   alembic upgrade head
   ```

6. **Run the application**
   ```bash
   uvicorn src.main:app --reload
   ```

7. **Access the API**
   - API Docs: http://localhost:8000/docs
   - Redoc: http://localhost:8000/redoc

### Running with Docker

1. **Build and start the services**
   ```bash
   docker-compose up --build
   ```

2. **Access the application**
   - API: http://localhost:8000
   - PostgreSQL: localhost:5432

## 📚 API Documentation

### Endpoints

- `GET /` - Welcome message
- `GET /health` - Health check
- `GET /api/consumption/` - Get consumption data
- `GET /api/consumption/summary` - Get consumption summary

## 🤝 Contributing

1. Fork the repository
2. Create a new branch (`git checkout -b feature/AmazingFeature`)
3. Commit your changes (`git commit -m 'Add some AmazingFeature'`)
4. Push to the branch (`git push origin feature/AmazingFeature`)
5. Open a Pull Request

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## 🙏 Acknowledgments

- [FastAPI](https://fastapi.tiangolo.com/)
- [Darts](https://unit8co.github.io/darts/)
- [SQLAlchemy](https://www.sqlalchemy.org/)
- [Pandas](https://pandas.pydata.org/)