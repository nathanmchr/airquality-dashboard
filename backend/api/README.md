# 🌿 France Air Quality API

A **FastAPI backend** providing access to air quality measurements
stored in PostgreSQL.\
It exposes endpoints to retrieve historical and latest values for
monitoring stations across France.

## 📘 Before you begin

If you plan to set up the project using **Docker**, please follow the general installation guide first:

👉 **See main setup documentation**:
[Main documentation](../../README.md)

This README only describes:

✅ how to run the API manually **without Docker**, for development or debugging

# 🐳 Setup With Docker (Recommended)

If you're running the full project using Docker (API + Frontend + ETL + Database), the API container will start automatically.

---

# 🖥️ Setup Without Docker (Manual Run)

This section explains how to run the API locally without any containers.

---

### 1. Install dependencies

```bash
cd backend/api
pip install -r ../requirements.txt
```

If missing, install manually:

```bash
pip install pandas sqlalchemy psycopg2-binary python-dotenv requests pytz
```

### 2. Create `.env`

In the etl folder, create a .env file containing these informations:

```bash
ETL_USERNAME=postgres
ETL_PASSWORD=postgres
DB_SERVER=localhost
DB_PORT=5432
DB_NAME=airquality_db
TABLE_NAME=air_quality_measurements
```

Note: the .env variables between modules must match name and values in order for the application to run properly.

### 3. Start API

```bash
uvicorn main:app --reload
```

# 🧩 Endpoints

## `GET /measurements/`

Retrieve measurements for a given range.

## `GET /measurements/latest`

Retrieve latest measurement per station.

# 🛠️ Tech Stack

- FastAPI
- SQLAlchemy

# 📄 License

GPL-3.0 license © 2025 --- France Air Quality Dashboard
