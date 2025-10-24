import pandas as pd
from sqlalchemy import create_engine, text, Table, MetaData
from sqlalchemy.dialects.postgresql import insert
from logger_config import get_logger


logger = get_logger(__name__)


def load(
    df: pd.DataFrame,
    db_username: str = "",
    db_password: str = "",
    db_server: str = "",
    db_name: str = "",
    table_name: str = "",
) -> None:
    """
    Send a Pandas DataFrame to a PostgreSQL database.

    This function inserts data from a Pandas DataFrame into a PostgreSQL table using SQLAlchemy.
    It automatically creates the table if it does not exist, adds any missing columns,
    and ensures that no duplicate rows are inserted based on the combination of
    (site_name, start_time, end_time).

    Parameters
    ----------
    df : pandas.DataFrame
        DataFrame containing the data to be inserted into the database.
    db_username : str
        Username for the PostgreSQL database.
    db_password : str
        Password for the PostgreSQL database.
    db_server : str
        Hostname or IP address of the PostgreSQL server.
    db_name : str
        Name of the target PostgreSQL database.
    table_name : str
        Name of the target table within the database.

    Returns
    -------
    None
        This function does not return any value. All operations are executed directly
        in the database.
    """
    engine = create_engine(
        f"postgresql+psycopg2://{db_username}:{db_password}@{db_server}/{db_name}"
    )
    logger.info(f"Using engine to send data to the database: {engine}")

    with engine.begin() as connection:
        # Verifying if the table exists, if not throwing an error
        table_exists = connection.execute(
            text(
                f"""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables
                    WHERE table_schema = 'public' AND table_name = '{table_name}'
                );
                """
            )
        ).scalar()
        if not table_exists:
            logger.info(
                f"Table '{table_name}' does not exist. Create it, set the ETL user as an owner and try running the programm again."
            )

        # Verifying fields that already exist in the database
        result = connection.execute(
            text(
                f"""
                SELECT column_name FROM information_schema.columns
                WHERE table_name = '{table_name}';
                """
            )
        )
        existing_columns = {row[0] for row in result}

        # Adding the missing fields to the database
        missing_columns = [col for col in df.columns if col not in existing_columns]
        for col in missing_columns:
            dtype = df[col].dtype
            if "int" in str(dtype):
                sql_type = "INTEGER"
            elif "float" in str(dtype):
                sql_type = "FLOAT"
            elif "datetime" in str(dtype):
                sql_type = "TIMESTAMP"
            else:
                sql_type = "TEXT"

            logger.warning(
                f"Column '{col}' missing in '{table_name}', creating it as {sql_type}."
            )
            connection.execute(
                text(f"ALTER TABLE {table_name} ADD COLUMN {col} {sql_type};")
            )

        # Verifying if the unique index already exists
        index_exists = connection.execute(
            text(
                f"""
                SELECT 1
                FROM pg_indexes
                WHERE schemaname = 'public'
                  AND tablename = '{table_name}'
                  AND indexname = 'idx_unique_site_time';
                """
            )
        ).first()

        if not index_exists:
            connection.execute(
                text(
                    f"""
                    CREATE UNIQUE INDEX idx_unique_site_time
                    ON {table_name} (site_name, start_time, end_time);
                    """
                )
            )
            logger.info("Unique index 'idx_unique_site_time' created.")

    # New transaction for the insertion
    with engine.begin() as connection:
        metadata = MetaData()
        table = Table(table_name, metadata, autoload_with=connection)

        if "site_name" in df.columns:
            df["site_name"] = df["site_name"].str.strip()

        # Preparing insert with on_conflict_do_nothing in order to still insert the new rows
        stmt = insert(table).values(df.to_dict(orient="records"))
        stmt = stmt.on_conflict_do_nothing(
            index_elements=["site_name", "start_time", "end_time"]
        )
        result_insert = connection.execute(stmt)
        inserted_count = (
            result_insert.rowcount if result_insert.rowcount is not None else 0
        )

    logger.info(
        f"{inserted_count} new rows inserted into '{table_name}' (duplicates ignored)."
    )
