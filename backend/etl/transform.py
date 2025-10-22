import pandas as pd
import logging
from io import StringIO
from typing import Optional

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def transform(
    measurements_text: str | None = None,
    locations_text: str | None = None,
    sep: str = ";",
) -> Optional[pd.DataFrame]:
    """
    Method for preparing a single, clean, and standardized DataFrame from two raw CSV texts: one containing air pollution measurements, and the other containing information about the locations where these measurements are taken.

    Args
    ----
        measurements_text(str): CSV text containing hourly aggregated air quality measurements.
        locations_text(str): CSV text containing metadata about the measurement locations, like their coordinates.
        sep(str): Separator used in the raw CSVs given.

    Returns
    -------
        df_merged(DataFrame): A cleaned and standardized Pandas DataFrame containing merged measurement and location information, or None if inputs are missing.
    """
    if measurements_text and locations_text:
        df_measurements = pd.read_csv(StringIO(measurements_text), sep=sep)
        df_measurements = df_measurements[
            [
                "Date de début",
                "Date de fin",
                "Organisme",
                "code site",
                "nom site",
                "Polluant",
                "valeur brute",
                "unité de mesure",
                "code qualité",
                "validité",
            ]
        ].copy()
        df_stations = pd.read_csv(StringIO(locations_text), sep=";")
        df_stations = df_stations[["Code", "Commune", "Longitude", "Latitude"]].copy()
        df_merged = pd.merge(
            df_measurements,
            df_stations,
            left_on="code site",
            right_on="Code",
            how="left",
        )
        df_merged = df_merged.drop(["code site", "Code"], axis=1)
        df_merged = df_merged.rename(
            columns={
                "Date de début": "start_time",
                "Date de fin": "end_time",
                "Organisme": "organization",
                "nom site": "site_name",
                "valeur brute": "raw_value",
                "Polluant": "pollutant",
                "unité de mesure": "unit",
                "code qualité": "quality_code",
                "validité": "validity",
                "Commune": "city",
                "Longitude": "longitude",
                "Latitude": "latitude",
            }
        )
        df_merged["start_time"] = pd.to_datetime(
            df_merged["start_time"], format="%Y/%m/%d %H:%M:%S", errors="coerce"
        )
        df_merged["end_time"] = pd.to_datetime(
            df_merged["end_time"], format="%Y/%m/%d %H:%M:%S", errors="coerce"
        )

        # Throwing warnings if coordinates or measurements are missing from the dataframe
        missing_coords = df_merged["longitude"].isna().sum()
        if missing_coords > 0:
            logger.warning(f"{missing_coords} stations are missing coordinates.")
        missing_measurements = df_merged["raw_value"].isna().sum()
        if missing_measurements > 0:
            logger.warning(f"{missing_measurements} missing measurements.")
        logger.info(f"Transformation successful — merged {len(df_merged)} rows.")
        return df_merged
    logger.error("Pass both raw CSV text for both the measurements and the locations.")
    return None
