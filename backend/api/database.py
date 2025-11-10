from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from .config import ETL_USERNAME, ETL_PASSWORD, DB_HOST, DB_PORT, DB_NAME, TABLE_NAME


db_username = ETL_USERNAME
db_password = ETL_PASSWORD
db_host = DB_HOST
db_port = DB_PORT
db_name = DB_NAME
table_name = TABLE_NAME

# SQLAlchemy engine for connecting to the PostgreSQL database
engine = create_engine(
    f"postgresql+psycopg2://{db_username}:{db_password}@{db_host}:{db_port}/{db_name}"
)

# Session factory for creating scoped database sessionss
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

# Declarative base class used for defining ORM models
Base = declarative_base()
