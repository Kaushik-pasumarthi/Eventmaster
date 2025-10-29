"""
Database connection and session management for PostgreSQL
"""
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, Session
from typing import Generator
import os
from dotenv import load_dotenv

load_dotenv()

# Get DATABASE_URL from environment, fallback to local PostgreSQL
DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql://prowess_user:prowess_password@localhost:5432/corporate_actions_db"
)

# Create engine with PostgreSQL-specific settings
engine = create_engine(
    DATABASE_URL,
    pool_pre_ping=True,  # Enable connection health checks
    pool_size=5,         # Number of connections to maintain
    max_overflow=10,     # Maximum number of connections that can be created beyond pool_size
    echo=False           # Set to True for SQL query logging (dev only)
)

# Create session factory
SessionLocal = sessionmaker(
    autocommit=False,
    autoflush=False,
    bind=engine
)


def get_db() -> Generator[Session, None, None]:
    """
    Dependency function to get database session for FastAPI endpoints
    
    Usage:
        @app.get("/endpoint")
        def endpoint(db: Session = Depends(get_db)):
            # use db here
    """
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


def init_db():
    """
    Initialize database - create all tables
    This should be called on application startup or via Alembic migrations
    """
    from models import Base
    Base.metadata.create_all(bind=engine)
