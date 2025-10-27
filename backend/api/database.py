from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from config import DB_USERNAME, DB_PASSWORD, DB_SERVER, DB_NAME, TABLE_NAME


db_username = DB_USERNAME
db_password = DB_PASSWORD
db_server = DB_SERVER
db_name = DB_NAME
table_name = TABLE_NAME

# SQLAlchemy engine for connecting to the PostgreSQL database
engine = create_engine(
    f"postgresql+psycopg2://{db_username}:{db_password}@{db_server}/{db_name}"
)

# Session factory for creating scoped database sessionss
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

# Declarative base class used for defining ORM models
Base = declarative_base()
