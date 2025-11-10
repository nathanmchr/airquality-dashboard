# 🌿 Air Quality Dashboard

A full-stack data project combining an ETL pipeline, a FastAPI backend,
and a React dashboard to monitor real-time air quality across France.

## Project Structure

    airquality-dashboard/
    ├── backend/                 # Python backend services
    │   ├── api/                # FastAPI application
    │   │   ├── config.py      # API configuration
    │   │   ├── database.py    # Database connection
    │   │   ├── main.py        # FastAPI routes & app
    │   │   └── models.py      # SQLAlchemy models
    │   └── etl/               # Data pipeline
    │       ├── config.py      # ETL configuration
    │       ├── extract.py     # Data extraction
    │       ├── transform.py   # Data transformation
    │       ├── load.py        # Database loading
    │       └── etl_pipeline.py # Pipeline orchestration
    ├── frontend/              # React frontend
    │   ├── src/
    │   │   ├── components/    # React components
    │   │   │   ├── Info.tsx   # Information display
    │   │   │   └── Map.tsx    # Map visualization
    │   │   ├── apiCalls.ts    # API integration
    │   │   └── App.tsx        # Main app component
    │   ├── public/            # Static assets
    │   └── package.json       # Frontend dependencies
    └── docker-compose.yml     # Container orchestration

# Setup Instructions

## Prerequisite: Geod’Air API Key

Before running the application, you must create an account on the Geod’Air platform and obtain an API key.

### Steps

- Go to the [Geod’Air API portal](http://www.geodair.fr/donnees/api).
- Create a new user account by filling the form on the page.
- Log in to your dashboard.
- Once on the API page while logged in, generate a new API key.
- Store the key securely.

## 🐳 Option 1: Run Everything with Docker (Recommended)

### 1. Create the `.env` file at the project root

```env
# ---  DB Settings ---
DB_NAME=airquality_db
DB_HOST=db
DB_PORT=5432
TABLE_NAME=air_quality_measurements

# --- PostgreSQL admin ---
POSTGRES_USERNAME=postgres
POSTGRES_PASSWORD=postgres_password

# ---  ETL/API User ---
ETL_USERNAME=etl_user
ETL_PASSWORD=etl_password

# ---  External Services ---
GEODAIR_API_KEY=your_api_key

# ---  Environment ---
ENVIRONMENT=production
```

### 2. Start the full stack

From the project root:

```bash
docker compose up --build
```

What this does:

- Starts PostgreSQL + initializes the database
- Creates the ETL user with correct permissions
- Executes `/db/init.sh`, i.e. configuring the postgreSQL database for this project (create superuser, etl user and credentials for this user)
- Launches the FastAPI backend
- Starts the ETL pipeline
- Serves the React frontend (if configured)

### 3. Access points

- API: http://localhost:8000
- Swagger UI: http://localhost:8000/docs
- PostgreSQL: `localhost:5432`

### 4. Reset the environment (if needed)

```bash
docker compose down -v
docker compose up --build
```

# 🖥️ Option 2: Run Without Docker (Manual Setup)

## 1. Setup PostgreSQL

```sql
CREATE DATABASE airquality_db;
CREATE USER etl_user WITH PASSWORD 'etl_password';
GRANT CONNECT ON DATABASE airquality_db TO etl_user;
GRANT USAGE ON SCHEMA public TO etl_user;
GRANT INSERT, SELECT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO etl_user;
```

The credentials used for this step are going to be used as environment variables for the different modules.

## 2. Backend Setup (FastAPI)

See [API documentation](/backend/api/README.md) for this step.

## 3. ETL Pipeline Setup

See [ETL documentation](/backend/etl/README.md) for this step.

## 4. Frontend Setup (React)

See [Frontend documentation](/frontend/README.md) for this step.
