import time, requests
import logging
from datetime import datetime
from typing import Optional
import pytz


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def extract_measurements(
    api_key: str = "",
    polluant_id: int = 39,
    date: str = "",
    max_retries: int = 6,
    wait_seconds: int = 5,
    timeout: int = 10,
) -> Optional[str]:
    """
    Method for extracting air quality data in France for a given polluant.

    Parameters
    ----------
        api_key : str
            Key for logging into GEOD'AIR's API.
        polluant_id : int
            Id for the pollutant requested (id for each pollutant can be found on the official GEOD'AIR API documentation). Default id (39) corresponds to PM2.5 particles.
        date : str
            Date (YYYY-MM-DD) of the day we want to get the measurements for the pollutant tracked.
        max_retries : int
            Number of attempts to fetch the data for the given arguments before considering the request a failure.
        wait_seconds : int
            Number of seconds we wait after a failed attempt before retrying.
        timeout : int
            Number of seconds of delay after which we consider the request as failed.

    Returns
    -------
        download : str
            CSV content (semicolon-separated) stored in a string variable containing the data for each location aggregated by hour, for every hour of the specified date.

    """

    if date == "":
        paris_tz = pytz.timezone("Europe/Paris")
        date = datetime.now(paris_tz).strftime("%Y-%m-%d")

    # First request to get the file id based on the polluant we want to track and what day we want to track it
    try:
        logger.info(f"Requesting data for Paris at date {date}.")
        id_request = requests.get(
            f"https://www.geodair.fr/api-ext/MoyH/export?date={date}&polluant={polluant_id}",
            headers={"accept": "text/plain", "apikey": api_key},
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
                headers={"apikey": api_key},
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


def extract_stations(api_key: str = "", date: str = ""):
    """
    Method for extracting stations informations data in France.

    Parameters
    ----------
        api_key : str
            Key for logging into GEOD'AIR's API.
        date : str
            Date (YYYY-MM-DD) of the day we want to get stations informations.

    Returns
    -------
        stations : str
            CSV content (semicolon-separated) stored in a string variable containing the data for each station at the specified date.

    """
    if date == "":
        paris_tz = pytz.timezone("Europe/Paris")
        date = datetime.now(paris_tz).strftime("%Y-%m-%d")
    # Request to get a table with the information of each measurement location
    try:
        logger.info(f"Requesting data for Paris at date {date}.")
        id_request = requests.get(
            f"https://www.geodair.fr/api-ext/station/export?date={date}",
            headers={"accept": "text/csv", "apikey": api_key},
        )
        id_request.raise_for_status()
    except requests.RequestException as e:
        logger.error(f"Erreur lors de la requête d'export : {e}")
        return None
    stations = id_request.text.strip()
    return stations
