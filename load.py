import duckdb
import logging
import time

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    filename='load.log'
)
logger = logging.getLogger(__name__)

#Time Range

year1 = range(2015, 2025)
month1 = range(1, 13)

def load_parquet_files():
    con = None
    try:
        con = duckdb.connect(database='emissions.duckdb', read_only=False)
        logger.info("Connected to DuckDB instance")


        # Green taxi Data Loading
        con.execute("DROP TABLE IF EXISTS green_taxi_data")
        con.execute("""
            CREATE TABLE green_taxi_data AS
            SELECT lpep_pickup_datetime AS pick_up_datetime,
                   lpep_dropoff_datetime AS drop_off_dt,
                   passenger_count,
                   trip_distance
            FROM read_parquet('https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2015-01.parquet')
            LIMIT 0
        """)
        logger.info("Created green_taxi_data table")

        for y in year1:
            for m in month1:
                m_text = f"{m:02d}"
                url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_{y}-{m_text}.parquet"
                logger.info(f"Loading {url}")
                print(url)

                con.execute(f"""
                    INSERT INTO green_taxi_data
                    SELECT lpep_pickup_datetime AS pick_up_datetime,
                           lpep_dropoff_datetime AS drop_off_dt,
                           passenger_count,
                           trip_distance
                    FROM read_parquet('{url}');
                """)
                time.sleep(30)

        # Yellow taxi
        con.execute("DROP TABLE IF EXISTS yellow_taxi_data")
        con.execute("""
            CREATE TABLE yellow_taxi_data AS
            SELECT tpep_pickup_datetime AS pick_up_datetime,
                   tpep_dropoff_datetime AS drop_off_dt,
                   passenger_count,
                   trip_distance
            FROM read_parquet('https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2015-01.parquet')
            LIMIT 0
        """)
        logger.info("Created yellow_taxi_data table")

        for y in year1:
            for m in month1:
                m_text = f"{m:02d}"
                url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{y}-{m_text}.parquet"
                logger.info(f"Loading {url}")
                print(url)

                con.execute(f"""
                    INSERT INTO yellow_taxi_data
                    SELECT tpep_pickup_datetime AS pick_up_datetime,
                           tpep_dropoff_datetime AS drop_off_dt,
                           passenger_count,
                           trip_distance
                    FROM read_parquet('{url}');
                """)
                time.sleep(30)

    except Exception as e:
        print(f"An error occurred: {e}")
        logger.error(f"An error occurred: {e}")

    finally:
        if con:
            con.close()
            logger.info("Closed DuckDB connection")

if __name__ == "__main__":
    load_parquet_files()

