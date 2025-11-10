import os
from dotenv import load_dotenv


load_dotenv()


ETL_USERNAME = os.getenv("ETL_USERNAME")
ETL_PASSWORD = os.getenv("ETL_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")
TABLE_NAME = os.getenv("TABLE_NAME")
