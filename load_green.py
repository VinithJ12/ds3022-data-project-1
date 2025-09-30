import duckdb
import logging
import time

# Configure logging to write info and error messages to a log file (load_green_2024.log)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    filename='load_green_2024.log'
)
logger = logging.getLogger(__name__)

# Target year and months to load
year1 = [2024]   # Only process data for 2024
month1 = range(1, 13)  # All 12 months

def load_green_2024():
    con = None
    try:
        # Connect to the DuckDB database file (creates file if it doesn’t exist)
        con = duckdb.connect(database='emissions.duckdb', read_only=False)
        logger.info("Connected to DuckDB for Green Taxi 2024 data")

        # Drop the table if it already exists, ensuring a clean load
        con.execute("DROP TABLE IF EXISTS green_taxi_data")

        # Define the first file (January 2024) to set up the schema
        first_file = f"https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2024-01.parquet"

        # Create an empty table with the required schema
        # Only keep the relevant columns we need for emissions analysis:
        #   - lpep_pickup_datetime → pick_up_datetime
        #   - lpep_dropoff_datetime → drop_off_dt
        #   - passenger_count
        #   - trip_distance
        #
        # LIMIT 0 ensures no rows are actually inserted here — just the schema.
        con.execute(f"""
            CREATE TABLE green_taxi_data AS
            SELECT lpep_pickup_datetime AS pick_up_datetime,
                   lpep_dropoff_datetime AS drop_off_dt,
                   passenger_count,
                   trip_distance
            FROM read_parquet('{first_file}')
            LIMIT 0
        """)
        logger.info("Created empty green_taxi_data table with 2024 schema")

        # Loop through each month in 2024 and load the data
        for m in month1:
            url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2024-{m:02d}.parquet"
            logger.info(f"Loading file: {url}")
            print(url)

            # Insert rows from the monthly parquet file into green_taxi_data
            # Apply date filters so only trips strictly within 2024 are inserted
            con.execute(f"""
                INSERT INTO green_taxi_data
                SELECT lpep_pickup_datetime AS pick_up_datetime,
                       lpep_dropoff_datetime AS drop_off_dt,
                       passenger_count,
                       trip_distance
                FROM read_parquet('{url}')
                WHERE lpep_pickup_datetime BETWEEN '2024-01-01' AND '2024-12-31'
                  AND lpep_dropoff_datetime BETWEEN '2024-01-01' AND '2024-12-31';
            """)
            
            # Sleep 5 seconds between requests to avoid overwhelming the server
            time.sleep(5)
            logger.info(f"Successfully loaded monthly file: {url}")

        # Verify load by counting the total number of rows inserted
        g_count = con.execute("SELECT COUNT(*) FROM green_taxi_data").fetchone()[0]
        logger.info(f"Total rows loaded for Green Taxi 2024: {g_count}")
        print(f"Green taxi 2024 rows loaded: {g_count}")

    except Exception as e:
        # Log and print any errors
        print(f"Error: {e}")
        logger.error(f"Error: {e}")
    finally:
        # Always close the database connection
        if con:
            con.close()
            logger.info("Closed DuckDB connection")

if __name__ == "__main__":
    load_green_2024()