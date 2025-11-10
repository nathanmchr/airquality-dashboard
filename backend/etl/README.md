# 🧩 ETL Pipeline - France Air Quality Dashboard

This module handles the **ETL (Extract, Transform, Load)** pipeline used to retrieve, clean, and store hourly air quality measurements from the **GEOD'AIR API** into a **PostgreSQL** database.

---

## 📘 Before you begin

If you plan to set up the project using **Docker**, please follow the general installation guide first:

👉 **See main setup documentation**:
[Main documentation](../../README.md)

This README only describes:

✅ how to run the ETL pipeline manually **without Docker**, for development or debugging

---

# 🐳 Setup With Docker (Recommended)

If you're running the full project using Docker (API + Frontend + ETL + Database), the ETL container will start automatically.

---

# 🖥️ Setup Without Docker (Manual Run)

This section explains how to run the ETL locally without any containers.

---

## 1. Install Python dependencies

```bash
cd backend/api
pip install -r ../requirements.txt
```

If missing, install manually:

```bash
pip install pandas sqlalchemy psycopg2-binary python-dotenv requests pytz
```

---

## 2. Create `.env` file

In the etl folder, create a .env file containing these informations:

```bash
GEODAIR_API_KEY=your_api_key
ETL_USERNAME=postgres
ETL_PASSWORD=postgres
DB_SERVER=localhost
DB_PORT=5432
DB_NAME=airquality_db
TABLE_NAME=air_quality_measurements
```

Note: the .env variables between modules must match name and values in order for the application to run properly.

## 3. Run the ETL manually

```bash
python etl_pipeline.py
```

---

# 🛠️ Tech Stack

- Pandas
- Requests
- SQLAlchemy

# 📄 License

GPL-3.0 license © 2025 --- France Air Quality Dashboard
