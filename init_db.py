import os
from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy_utils import database_exists, create_database

# Load environment variables
load_dotenv()

def init_database():
    # Get database URL from environment variables
    db_url = os.getenv("DATABASE_URL")
    if not db_url:
        raise ValueError("DATABASE_URL environment variable not set")
    
    # Create database if it doesn't exist
    if not database_exists(db_url):
        print(f"Creating database: {db_url}")
        create_database(db_url)
        print("Database created successfully")
    else:
        print("Database already exists")

if __name__ == "__main__":
    init_database()
