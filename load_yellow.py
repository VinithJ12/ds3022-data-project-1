import duckdb
import logging
import time

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    filename='load_yellow.log'
)
logger = logging.getLogger(__name__)

year1 = range(2015, 2025)
month1 = range(1, 13)

def load_yellow_and_csv():
    con = None
    try:
        con = duckdb.connect(database='emissions.duckdb', read_only=False)
        logger.info("Connected to DuckDB for yellow taxi + CSV")

        # Yellow taxi table
        con.execute("DROP TABLE IF EXISTS yellow_taxi_data")
        con.execute("""
            CREATE TABLE yellow_taxi_data AS
            SELECT tpep_pickup_datetime AS pick_up_datetime,
                   tpep_dropoff_datetime AS drop_off_dt,
                   passenger_count,
                   trip_distance
            FROM read_parquet('https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2015-01.parquet')
            WHERE tpep_pickup_datetime BETWEEN '2015-01-01' AND '2024-12-31'
              AND tpep_dropoff_datetime BETWEEN '2015-01-01' AND '2024-12-31'
            LIMIT 0
        """)
        logger.info("Created yellow_taxi_data table")

        for y in year1:
            for m in month1:
                url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{y}-{m:02d}.parquet"
                logger.info(f"Loading {url}")
                print(url)

                con.execute(f"""
                    INSERT INTO yellow_taxi_data
                    SELECT tpep_pickup_datetime AS pick_up_datetime,
                           tpep_dropoff_datetime AS drop_off_dt,
                           passenger_count,
                           trip_distance
                    FROM read_parquet('{url}')
                    WHERE tpep_pickup_datetime BETWEEN '2015-01-01' AND '2024-12-31'
                      AND tpep_dropoff_datetime BETWEEN '2015-01-01' AND '2024-12-31';
                """)
                time.sleep(60)

        # Vehicle emissions CSV
        con.execute("DROP TABLE IF EXISTS vehicle_emissions")
        con.execute("""
            CREATE TABLE vehicle_emissions AS
            SELECT * FROM read_csv_auto('data/vehicle_emissions.csv')
        """)
        logger.info("Created vehicle_emissions table")

        # Row counts
        y_count = con.execute("SELECT COUNT(*) FROM yellow_taxi_data").fetchone()[0]
        e_count = con.execute("SELECT COUNT(*) FROM vehicle_emissions").fetchone()[0]
        logger.info(f"Yellow taxi rows loaded: {y_count}")
        logger.info(f"Vehicle emissions rows loaded: {e_count}")

    except Exception as e:
        print(f"Error: {e}")
        logger.error(f"Error: {e}")
    finally:
        if con:
            con.close()
            logger.info("Closed DuckDB connection")

if __name__ == "__main__":
    load_yellow_and_csv()