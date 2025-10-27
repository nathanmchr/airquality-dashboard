from sqlalchemy import DateTime, Column, Integer, Float, String
from database import Base


class AirQualityMeasurements(Base):
    """
    SQLAlchemy ORM model representing a single air quality measurement record.

    This table stores hourly pollutant measurements collected from various
    monitoring stations, along with their associated metadata such as
    organization, location, and quality indicators.

    Attributes
    ----------
    id : int
        Unique identifier for the measurement record (primary key).
    start_time : datetime
        Beginning of the measurement period.
    end_time : datetime
        End of the measurement period.
    organization : str
        Name of the organization or agency that collected the data (e.g. "ATMO GRAND EST").
    site_name : str
        Name of the measurement site or station.
    pollutant : str
        Type of pollutant measured (e.g. "PM2.5", "NO2", "O3").
    raw_value : float, optional
        Recorded measurement value. Can be null if data is missing or invalid.
    unit : str, optional
        Unit of measurement, usually expressed in micrograms per cubic meter (e.g. "µg/m³").
    quality_code : str, optional
        Quality flag of the measurement (e.g. "A" for validated, "R" for raw, "N" for not validated).
    validity : int, optional
        Indicator of the measurement’s validity (1 = valid, -1 = invalid, etc.).
    city : str, optional
        Name of the city associated with the measurement station.
    longitude : float, optional
        Geographical longitude of the measurement station.
    latitude : float, optional
        Geographical latitude of the measurement station.
    """

    __tablename__ = "air_quality_measurements"

    start_time = Column(DateTime, nullable=False, index=True)
    end_time = Column(DateTime, nullable=False)
    organization = Column(String, nullable=False)
    site_name = Column(String, nullable=False, index=True)
    pollutant = Column(String, nullable=False, index=True)
    raw_value = Column(Float, nullable=True)
    unit = Column(String, nullable=True)
    quality_code = Column(String, nullable=True)
    validity = Column(Integer, nullable=True)
    city = Column(String, nullable=True, index=True)
    longitude = Column(Float, nullable=True)
    latitude = Column(Float, nullable=True)
    id = Column(Integer, primary_key=True, index=True)
