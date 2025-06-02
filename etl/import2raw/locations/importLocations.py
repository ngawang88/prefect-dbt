import os
import glob
import pandas as pd
from prefect import task, get_run_logger
from shared.loadCSV.loadCSV import load_csv_to_df
from shared.dbUtils.src.shared.db_writer import db_writer

@task
def import_locations(ctx=None):
    logger = get_run_logger()
    logger.info("Running locations import task...")
    # Find the locations CSV file(s)
    redcat_dir = os.environ.get("REDCAT_DIR", "./rawFile/redcat")
    pattern = os.path.join(redcat_dir, "locations_*.csv")
    files = glob.glob(pattern)
    logger.info(f"Found {len(files)} locations files: {files}")
    if not files:
        logger.warning("No locations files found. Skipping.")
        return None
    all_locations = []
    for file in files:
        try:
            df = load_csv_to_df(file)
            logger.info(f"Read {len(df)} rows from {file}")
            all_locations.append(df)
        except Exception as e:
            logger.error(f"Error processing {file}: {e}")
    if not all_locations:
        logger.warning("No valid locations data loaded.")
        return None
    locations_df = pd.concat(all_locations, ignore_index=True)
    logger.info(f"Total locations loaded: {len(locations_df)}")
    # Ensure date columns are strings in 'YYYY-MM-DD' format or NULL
    for col in ["DateOpened", "DateClosed"]:
        if col in locations_df.columns:
            locations_df[col] = pd.to_datetime(locations_df[col], errors='coerce')
            locations_df[col] = locations_df[col].dt.strftime('%Y-%m-%d')
            locations_df[col] = locations_df[col].replace(['NaT', 'nan'], [None, None])
    # Truncate the temp table before loading new data
    try:
        db_writer(sql_command="TRUNCATE TABLE public.temp_rc_rawlocations;")
        logger.info("Truncated temp_rc_rawlocations table.")
    except Exception as e:
        logger.warning(f"Could not truncate temp_rc_rawlocations (may not exist yet): {e}")
    # Save DataFrame to a temp table in Redshift
    try:
        db_writer(df=locations_df, table_name="temp_rc_rawlocations", schema='public', if_exists="append")
        logger.info("Successfully wrote locations data to Redshift temp table public.temp_rc_rawlocations.")
    except Exception as e:
        logger.error(f"Failed to write locations data to Redshift temp table: {e}")
        return None
    # Now run the upsert SQL file (assumed to be in etl/import2raw/locations/sql/upsert.sql)
    try:
        db_writer(sql_file="etl/import2raw/locations/sql/upsert.sql")
        logger.info("Successfully upserted data from temp_rc_rawlocations to rc_rawlocations using upsert.sql.")
    except Exception as e:
        logger.error(f"Failed to upsert locations data in Redshift: {e}")
        return None
    return locations_df
