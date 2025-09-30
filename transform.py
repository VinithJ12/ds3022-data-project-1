import duckdb
import logging

#Setting up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    filename='transform.log'
)
logger = logging.getLogger(__name__)



def transform_taxi_data():
    #Connecting to Duckdb database
    con = duckdb.connect(database='emissions.duckdb', read_only=False)
    logger.info("Connected to emissions.duckdb")

    try:
        con.execute("""
        CREATE OR REPLACE TABLE taxi_trips_transformed AS
        SELECT
            'yellow' AS taxi_type,
            t.pick_up_datetime,
            t.drop_off_dt,
            t.passenger_count,
            t.trip_distance,
            
            
            EXTRACT(EPOCH FROM (t.drop_off_dt - t.pick_up_datetime)) AS trip_duration_seconds,
            t.trip_distance / (EXTRACT(EPOCH FROM (t.drop_off_dt - t.pick_up_datetime)) / 3600) AS avg_mph,  -- Average speed in mph
            (t.trip_distance * v.co2_grams_per_mile)/1000 AS co2_kg_per_trip,
            
            EXTRACT(HOUR FROM t.pick_up_datetime) AS trip_hour,
            EXTRACT(DOW FROM t.pick_up_datetime) AS trip_day_of_week,
            EXTRACT(WEEK FROM t.pick_up_datetime) AS trip_week,
            EXTRACT(MONTH FROM t.pick_up_datetime) AS trip_month
        
        FROM yellow_taxi_data t
        LEFT JOIN vehicle_emissions v
            ON v.vehicle_type = 'yellow_taxi'
        WHERE t.drop_off_dt > t.pick_up_datetime

        UNION ALL

        SELECT
            'green' AS taxi_type,
            t.pick_up_datetime,
            t.drop_off_dt,
            t.passenger_count,
            t.trip_distance,
            
            EXTRACT(EPOCH FROM (t.drop_off_dt - t.pick_up_datetime)) AS trip_duration_seconds,
            t.trip_distance / (EXTRACT(EPOCH FROM (t.drop_off_dt - t.pick_up_datetime)) / 3600) AS avg_mph,  -- Average speed in mph
            (t.trip_distance * v.co2_grams_per_mile)/1000 AS co2_kg_per_trip,
            EXTRACT(HOUR FROM t.pick_up_datetime) AS trip_hour,
            EXTRACT(DOW FROM t.pick_up_datetime) AS trip_day_of_week,
            EXTRACT(WEEK FROM t.pick_up_datetime) AS trip_week,
            EXTRACT(MONTH FROM t.pick_up_datetime) AS trip_month
        
        FROM green_taxi_data t
        LEFT JOIN vehicle_emissions v
            ON v.vehicle_type = 'green_taxi'
        WHERE t.drop_off_dt > t.pick_up_datetime
        """)
        logger.info("Transformation complete: saved as 'taxi_trips_transformed'")
        print("Transformation complete..Finally!")

    except Exception as e:
        logger.error(f"Error during transformation: {e}")
        print("Error during transformation:", e)

    finally:
        con.close()
        logger.info("DuckDB connection closed")

if __name__ == "__main__":
    transform_taxi_data()

