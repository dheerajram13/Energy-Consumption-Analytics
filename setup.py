from setuptools import setup, find_packages

setup(
    name="energy_analytics",
    version="0.1.0",
    packages=find_packages(where="src"),
    package_dir={"": "src"},
    install_requires=[
        "fastapi>=0.68.0",
        "uvicorn>=0.15.0",
        "python-dotenv>=0.19.0",
        "sqlalchemy>=1.4.23",
        "psycopg2-binary>=2.9.1",
        "alembic>=1.7.3",
        "pandas>=1.3.5,<2.0.0",
        "numpy>=1.21.2,<2.0.0",
        "python-jose[cryptography]>=3.3.0",
        "passlib[bcrypt]>=1.7.4",
        "python-multipart>=0.0.5",
        "darts[torch]>=0.25.0,<0.26.0",
        "scikit-learn>=1.0.1,<2.0.0",
        "cmdstanpy>=1.0.4,<2.0.0",
        "pmdarima==1.8.5",
    ],
    python_requires=">=3.9",
)
