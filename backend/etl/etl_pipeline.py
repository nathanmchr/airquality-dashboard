from config import API_KEY, DB_USERNAME, DB_PASSWORD, DB_SERVER, DB_NAME, TABLE_NAME
from extract import extract_measurements, extract_stations
from pytz import timezone
from transform import transform
from load import load
from datetime import datetime, timedelta
from typing import TypedDict
from logger_config import get_logger
import sys
import time


class Config(TypedDict):
    api_key: str
    db_username: str
    db_password: str
    db_server: str
    db_name: str
    table_name: str


logger = get_logger(__name__)


def get_config() -> Config:
    raw_config: dict[str, str | None] = {
        "api_key": API_KEY,
        "db_username": DB_USERNAME,
        "db_password": DB_PASSWORD,
        "db_server": DB_SERVER,
        "db_name": DB_NAME,
        "table_name": TABLE_NAME,
    }

    missing = [k for k, v in raw_config.items() if not v]
    if missing:
        raise ValueError(f"Missing environment variables: {', '.join(missing)}")

    return raw_config  # type: ignore[return-value]


def run_etl() -> None:
    """
    Run the complete ETL pipeline.

    This function retrieves air quality measurements and location data from an external API,
    then merges and cleans them into a single Pandas DataFrame. The resulting DataFrame is
    then loaded into a PostgreSQL database.

    Parameters
    ----------
    None
        This function takes no parameters. It relies on global variables defined in the .env file.

    Returns
    -------
    None
        Nothing is returned, as the processed data is sent directly to the PostgreSQL database.
    """
    try:
        logger.info(f"Starting ETL cycle at {datetime.now()}")

        config = get_config()
        # Extract
        measurements = extract_measurements(api_key=config["api_key"])
        if measurements is None:
            raise RuntimeError("Failed to fetch measurements from API.")
        locations = extract_stations(api_key=config["api_key"])
        if locations is None:
            raise RuntimeError("Failed to fetch measurements from API.")

        # Transform
        df = transform(measurements_text=measurements, locations_text=locations)
        if df is None:
            raise RuntimeError(
                "Failed to create dataframe from measurements and locations."
            )

        # Load
        load(
            df,
            db_username=config["db_username"],
            db_password=config["db_password"],
            db_server=config["db_server"],
            db_name=config["db_name"],
            table_name=config["table_name"],
        )
        logger.info(f"ETL cycle completed at {datetime.now()}")

    except Exception as e:
        logger.error(f"ETL failed: {e}")


if __name__ == "__main__":

    # Defining timezone for France (data is fetched using french date)
    paris_tz = timezone("Europe/Paris")

    # First run at start
    run_etl()

    # Loop for running the ETL at every hour +1 minutes
    try:
        while True:
            now = datetime.now(paris_tz)
            next_run = (now + timedelta(hours=1)).replace(
                minute=1, second=0, microsecond=0
            )
            sleep_time = max((next_run - now).total_seconds(), 0)
            logger.info(
                f"Next ETL scheduled for {next_run.strftime('%Y-%m-%d %H:%M:%S')} Paris time "
                f"({sleep_time/60:.1f} minutes from now)."
            )
            time.sleep(sleep_time)
            run_etl()
    except KeyboardInterrupt:
        logger.info("ETL process stopped manually.")
        sys.exit(0)
