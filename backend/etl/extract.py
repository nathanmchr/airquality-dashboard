import time, requests, os
from datetime import datetime
from dotenv import load_dotenv
from typing import Optional
import logging


load_dotenv()
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

API_KEY = os.getenv("GEODAIR_API_KEY")
if not API_KEY:
    raise SystemExit(
        "GEODAIR_API_KEY missing. Please add it in the .env configuration file."
    )


def extract_measurements(
    polluant_id: int = 39,
    date: str | None = None,
    max_retries: int = 6,
    wait_seconds: int = 5,
    timeout: int = 10,
) -> Optional[str]:
    """
    Method for extracting air quality data in France for a given polluant.

    Args
    ----
        polluant_id(int): Id for the pollutant requested (id for each pollutant can be found on the official GEOD'AIR API documentation). Default id (39) corresponds to PM2.5 particles.
        date(str): Date (YYYY-MM-DD) of the day we want to get the measurements for the pollutant tracked.
        max_retries(int): Number of attempts to fetch the data for the given arguments before considering the request a failure.
        wait_seconds(int): Number of seconds we wait after a failed attempt before retrying.
        timeout(int): Number of seconds of delay after which we consider the request as failed.

    Returns
    -------
        download(str): CSV content (semicolon-separated) stored in a string variable containing the data for each location aggregated by hour, for every hour of the specified date.

    """
    if date is None:
        date = datetime.today().strftime("%Y-%m-%d")

    # First request to get the file id based on the polluant we want to track and what day we want to track it
    try:
        id_request = requests.get(
            f"https://www.geodair.fr/api-ext/MoyH/export?date={date}&polluant={polluant_id}",
            headers={"accept": "text/plain", "apikey": API_KEY},
        )
        id_request.raise_for_status()
    except requests.RequestException as e:
        logger.error(f"Erreur lors de la requête d'export : {e}")
        return None
    file_id = id_request.text.strip()

    # Second request to get the file from the corresponding id we just fetched
    for attempt in range(1, max_retries + 1):
        try:
            dl = requests.get(
                f"https://www.geodair.fr/api-ext/download?id={file_id}",
                headers={"apikey": API_KEY},
                timeout=timeout,
            )
            dl.raise_for_status()
            download = dl.text.strip()
            return download
        except requests.RequestException as e:
            logger.warning(
                f"Attempt {attempt}/{max_retries} failed: {e}. Retrying in {wait_seconds}s..."
            )
            time.sleep(wait_seconds)

    logger.error("Failure : max attempts number reached.")
    return None


def extract_stations(date: str | None = None):
    if date is None:
        date = datetime.today().strftime("%Y-%m-%d")
    # Request to get a table with the information of each measurement location
    try:
        id_request = requests.get(
            f"https://www.geodair.fr/api-ext/station/export?date={date}",
            headers={"accept": "text/csv", "apikey": API_KEY},
        )
        id_request.raise_for_status()
    except requests.RequestException as e:
        logger.error(f"Erreur lors de la requête d'export : {e}")
        return None
    stations = id_request.text.strip()
    return stations
