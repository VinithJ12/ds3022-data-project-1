import duckdb
import logging
import time

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    filename='load_green.log'
)
logger = logging.getLogger(__name__)

year1 = range(2015, 2025)
month1 = range(1, 13)

def load_green():
    con = None
    try:
        con = duckdb.connect(database='emissions.duckdb', read_only=False)
        logger.info("Connected to DuckDB for green taxi")

        # Green taxi table
        con.execute("DROP TABLE IF EXISTS green_taxi_data")
        con.execute("""
            CREATE TABLE green_taxi_data AS
            SELECT lpep_pickup_datetime AS pick_up_datetime,
                   lpep_dropoff_datetime AS drop_off_dt,
                   passenger_count,
                   trip_distance
            FROM read_parquet('https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2015-01.parquet')
            WHERE lpep_pickup_datetime BETWEEN '2015-01-01' AND '2024-12-31'
              AND lpep_dropoff_datetime BETWEEN '2015-01-01' AND '2024-12-31'
            LIMIT 0
        """)
        logger.info("Created green_taxi_data table")

        for y in year1:
            for m in month1:
                url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_{y}-{m:02d}.parquet"
                logger.info(f"Loading {url}")
                print(url)

                con.execute(f"""
                    INSERT INTO green_taxi_data
                    SELECT lpep_pickup_datetime AS pick_up_datetime,
                           lpep_dropoff_datetime AS drop_off_dt,
                           passenger_count,
                           trip_distance
                    FROM read_parquet('{url}')
                    WHERE lpep_pickup_datetime BETWEEN '2015-01-01' AND '2024-12-31'
                      AND lpep_dropoff_datetime BETWEEN '2015-01-01' AND '2024-12-31';
                """)
                time.sleep(25)

        g_count = con.execute("SELECT COUNT(*) FROM green_taxi_data").fetchone()[0]
        logger.info(f"Green taxi rows loaded: {g_count}")

    except Exception as e:
        print(f"Error: {e}")
        logger.error(f"Error: {e}")
    finally:
        if con:
            con.close()
            logger.info("Closed DuckDB connection")

if __name__ == "__main__":
    load_green()