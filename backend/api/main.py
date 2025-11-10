from fastapi import FastAPI, HTTPException, Depends, Query
from fastapi.middleware.gzip import GZipMiddleware
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import Annotated, Optional
from datetime import datetime
from .database import SessionLocal
from sqlalchemy.orm import Session
from sqlalchemy import text, func
from .models import AirQualityMeasurements
import math

app = FastAPI()
app.add_middleware(GZipMiddleware, minimum_size=1000)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # or the address of the frontend server
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


class Measurement(BaseModel):
    """
    Represents an air quality measurement.
    Fields match the PostgreSQL air quality table .

    Attributes
    ----------
    id : int
        Unique identifier of the measurement.
    start_time : datetime
        Start of the measurement period.
    end_time : datetime
        End of the measurement period.
    organization : str
        Name of the agency that produced the measurement (e.g., "ATMO GRAND EST").
    site_name : str
        Name of the measurement site or station.
    pollutant : str
        The pollutant being measured (e.g., "PM2.5", "O3", etc.).
    raw_value : float, optional
        The measured value (may be NaN if data is missing).
    unit : str, optional
        Unit of measurement (commonly "µg/m³").
    quality_code : str, optional
        Quality flag of the measurement (e.g., "A", "R", "N").
    validity : int, optional
        Validity indicator of the measurement (1 = valid, -1 = invalid, etc.).
    city : str, optional
        Name of the city associated with the measurement station.
    longitude : float, optional
        Geographic longitude coordinate of the station.
    latitude : float, optional
        Geographic latitude coordinate of the station.
    """

    id: int
    start_time: datetime
    end_time: datetime
    organization: str
    site_name: str
    pollutant: str
    raw_value: Optional[float]
    unit: Optional[str]
    quality_code: Optional[str]
    validity: Optional[int]
    city: Optional[str]
    longitude: Optional[float]
    latitude: Optional[float]


def get_db():
    """
    Dependency that provides a SQLAlchemy database session.
    Ensures the session is properly closed after the request.
    """
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


# On convertit les NaN en None (JSON-friendly)
def clean(obj):
    """
    Converts NaN float values into None to make them JSON serializable.

    Parameters
    ----------
    obj : Any
        The object to clean.

    Returns
    -------
    Any
        The cleaned object (None if NaN, otherwise unchanged).
    """
    if isinstance(obj, float) and math.isnan(obj):
        return None
    return obj


db_dependency = Annotated[Session, Depends(get_db)]


@app.get("/measurements/")
async def get_measurements(
    db: db_dependency,
    start_time: datetime = Query(
        ...,
        description="Start of the period to query (format: YYYY-MM-DD HH:MM)",
        example="2025-10-23 00:00",
    ),
    end_time: datetime = Query(
        ...,
        description="End of the period to query (format: YYYY-MM-DD HH:MM)",
        example="2025-10-24 00:00",
    ),
    site_name: str | None = Query(
        None,
        description="Optional site name to filter measurements by",
        example="PARIS 18eme",
    ),
):
    """
    Retrieve air quality measurements filtered by time period and optionally by site.

    Query Parameters
    ----------------
    start_time : datetime
        The start of the period to query.
    end_time : datetime
        The end of the period to query.
    site_name : str, optional
        The name of the site to filter by.

    Returns
    -------
    list[dict]
        A list of measurement objects serialized as dictionaries.

    Raises
    ------
    HTTPException
        If no measurements are found for the given filters.
    """
    if not start_time or not end_time:
        raise HTTPException(
            status_code=400, detail="Pass start and end for the period of the request."
        )
    if start_time > end_time:
        raise HTTPException(
            status_code=400, detail="start_time must be earlier than end_time"
        )

    query = (
        db.query(AirQualityMeasurements)
        .filter(AirQualityMeasurements.start_time >= start_time)
        .filter(AirQualityMeasurements.end_time <= end_time)
    )

    if site_name:
        query = query.filter(AirQualityMeasurements.site_name == site_name)

    results = query.all()

    if not results:
        raise HTTPException(status_code=404, detail="Measurements not found.")
    cleaned = []
    for row in results:
        cleaned.append(
            {
                "id": row.id,
                "start_time": row.start_time,
                "end_time": row.end_time,
                "organization": row.organization,
                "site_name": row.site_name,
                "pollutant": row.pollutant,
                "raw_value": clean(row.raw_value),
                "unit": row.unit,
                "quality_code": row.quality_code,
                "validity": row.validity,
                "city": row.city,
                "longitude": clean(row.longitude),
                "latitude": clean(row.latitude),
            }
        )

    return cleaned


@app.get("/measurements/latest")
async def get_latest_measurements(db: Session = Depends(get_db)):
    """
    Retrieve the latest measurement for each site.

    Returns
    -------
    list[dict]
        The most recent measurement per site.
    """

    # Subquery for finging latest measurement time per site
    subquery = (
        db.query(
            AirQualityMeasurements.site_name,
            func.max(AirQualityMeasurements.end_time).label("latest_time"),
        )
        .group_by(AirQualityMeasurements.site_name)
        .subquery()
    )

    # Join to get the corresponding rows
    query = (
        db.query(AirQualityMeasurements)
        .join(
            subquery,
            (AirQualityMeasurements.site_name == subquery.c.site_name)
            & (AirQualityMeasurements.end_time == subquery.c.latest_time),
        )
        .order_by(AirQualityMeasurements.site_name.asc())
    )

    results = query.all()

    if not results:
        raise HTTPException(status_code=404, detail="No latest measurements found.")

    cleaned = [
        {
            "id": row.id,
            "start_time": row.start_time,
            "end_time": row.end_time,
            "organization": row.organization,
            "site_name": row.site_name,
            "pollutant": row.pollutant,
            "raw_value": clean(row.raw_value),
            "unit": row.unit,
            "quality_code": row.quality_code,
            "validity": row.validity,
            "city": row.city,
            "longitude": clean(row.longitude),
            "latitude": clean(row.latitude),
        }
        for row in results
    ]

    return cleaned
