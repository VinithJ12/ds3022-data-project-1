import duckdb
import logging
import time

# Configure logging to capture info and error messages into a log file (load_yellow_2024.log)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    filename='load_yellow_2024.log'
)
logger = logging.getLogger(__name__)

# Target year and months to process
year1 = [2024]   # Only process data for 2024
month1 = range(1, 13)  # All 12 months

def load_yellow_2024_and_csv():
    """
    Load 2024 Yellow Taxi trip data and a vehicle emissions CSV file into DuckDB.

    Workflow:
    1. Connect to DuckDB database (emissions.duckdb).
    2. Drop & recreate the `yellow_taxi_data` table with the required schema.
    3. Loop through all 12 monthly parquet files for 2024 and insert rows.
    4. Drop & recreate the `vehicle_emissions` table from a local CSV file.
    5. Log row counts for both datasets.
    """
    con = None
    try:
        # Connect to DuckDB database (creates if it doesn’t exist).
        # Using the same file as other loaders for integration.
        con = duckdb.connect(database='emissions.duckdb', read_only=False)
        logger.info("Connected to DuckDB for Yellow Taxi + Vehicle Emissions (2024)")

        # Create Yellow Taxi Table
        # Drop any existing yellow_taxi_data table so we start fresh
        con.execute("DROP TABLE IF EXISTS yellow_taxi_data")

        # Define January 2024 parquet file to establish schema
        first_file = f"https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet"

        # Create table structure by selecting only the relevant columns:
        #   - tpep_pickup_datetime → pick_up_datetime
        #   - tpep_dropoff_datetime → drop_off_dt
        #   - passenger_count
        #   - trip_distance
        # LIMIT 0 ensures no rows are inserted, just the schema definition.
        con.execute(f"""
            CREATE TABLE yellow_taxi_data AS
            SELECT tpep_pickup_datetime AS pick_up_datetime,
                   tpep_dropoff_datetime AS drop_off_dt,
                   passenger_count,
                   trip_distance
            FROM read_parquet('{first_file}')
            LIMIT 0
        """)
        logger.info("Created empty yellow_taxi_data table with 2024 schema")

        # Load Yellow Taxi Monthly Data
        for m in month1:
            url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-{m:02d}.parquet"
            logger.info(f"Loading file: {url}")
            print(url)

            # Insert rows into yellow_taxi_data
            # Apply filters so only trips fully within 2024 are included
            con.execute(f"""
                INSERT INTO yellow_taxi_data
                SELECT tpep_pickup_datetime AS pick_up_datetime,
                       tpep_dropoff_datetime AS drop_off_dt,
                       passenger_count,
                       trip_distance
                FROM read_parquet('{url}')
                WHERE tpep_pickup_datetime BETWEEN '2024-01-01' AND '2024-12-31'
                  AND tpep_dropoff_datetime BETWEEN '2024-01-01' AND '2024-12-31';
            """)
            
            # Sleep 5 seconds between file loads (server-friendly and avoids rate limiting)
            time.sleep(5)

        # Load Vehicle Emissions Reference Data
        # Drop and recreate so it matches the latest CSV each run
        con.execute("DROP TABLE IF EXISTS vehicle_emissions")
        con.execute("""
            CREATE TABLE vehicle_emissions AS
            SELECT * FROM read_csv_auto('data/vehicle_emissions.csv')
        """)
        logger.info("Created vehicle_emissions table from CSV")

        #Row Counts for Validation
        y_count = con.execute("SELECT COUNT(*) FROM yellow_taxi_data").fetchone()[0]
        e_count = con.execute("SELECT COUNT(*) FROM vehicle_emissions").fetchone()[0]

        logger.info(f"Yellow taxi 2024 rows loaded: {y_count}")
        logger.info(f"Vehicle emissions rows loaded: {e_count}")
        print(f"Yellow taxi 2024 rows loaded: {y_count}")
        print(f"Vehicle emissions rows loaded: {e_count}")

    except Exception as e:
        # Log and print any errors for troubleshooting
        print(f"Error: {e}")
        logger.error(f"Error: {e}")
    finally:
        # Always close the connection to avoid DB locks
        if con:
            con.close()
            logger.info("Closed DuckDB connection")

if __name__ == "__main__":
    load_yellow_2024_and_csv()

