import duckdb
import logging
import pandas as pd

# Logging Setup
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    filename='clean_2024.log'
)
logger = logging.getLogger(__name__)

# This is the Timeframe we are working with
YEAR_START = '2024-01-01'
YEAR_END = '2024-12-31'

# Let's define a function that will clean and verify the cleaned components
def clean_and_verify_chunked():
    con = duckdb.connect(database='emissions.duckdb', read_only=False)
    logger.info("Connected to emissions.duckdb for cleaning and verification")
    
    # Taxi tables
    taxis = ["yellow_taxi_data", "green_taxi_data"]  # These are the two tables we created in the load step
    
    # Monthly chunks
    dates = pd.date_range(start=YEAR_START, end=YEAR_END, freq='MS')
    
    for taxi in taxis:
        clean_table = f"clean_{taxi}"
        
        # Create clean table
        con.execute(f"""
            CREATE TABLE IF NOT EXISTS {clean_table} (
                pick_up_datetime TIMESTAMP,
                drop_off_dt TIMESTAMP,
                passenger_count INT,
                trip_distance DOUBLE
            )
        """)
        logger.info(f"Created {clean_table} table (if not exists)")

        invalid_count = 0  # Counter for the invalid rows across months
        
        # CHATGPT helped me frame this for loop because my terminal kept throwing a "Killed" statement 
        # and it offered a step of chunking by months.
        for start, end in zip(dates, dates[1:].append(pd.Index([pd.Timestamp(YEAR_END)]))):
            # Insert cleaned data per month
            con.execute(f"""
                INSERT INTO {clean_table}
                SELECT DISTINCT pick_up_datetime, drop_off_dt, passenger_count, trip_distance 
                FROM {taxi}
                WHERE pick_up_datetime >= '{start}' AND pick_up_datetime < '{end}'
                  AND drop_off_dt IS NOT NULL
                  AND passenger_count > 0
                  AND trip_distance > 0 
                  AND trip_distance <= 100
                  AND (drop_off_dt - pick_up_datetime) <= INTERVAL '24 hours';
            """)
            logger.info(f"Inserted cleaned {taxi} data from {start.date()} to {end.date()}")
            
            # Verify invalid rows for this month
            issues = con.execute(f"""
                SELECT COUNT(*) FROM {clean_table}
                WHERE pick_up_datetime IS NULL
                   OR drop_off_dt IS NULL
                   OR passenger_count <= 0
                   OR trip_distance <= 0 OR trip_distance > 100
                   OR (drop_off_dt - pick_up_datetime) > INTERVAL '24 hours'
                   OR pick_up_datetime NOT BETWEEN '{YEAR_START}' AND '{YEAR_END}'
                   OR drop_off_dt NOT BETWEEN '{YEAR_START}' AND '{YEAR_END}';
            """).fetchone()[0]
            invalid_count += issues

        # Replace original table with cleaned table (swing method)
        con.execute(f"DROP TABLE IF EXISTS {taxi}")
        con.execute(f"ALTER TABLE {clean_table} RENAME TO {taxi}")
        logger.info(f"{taxi} cleaned and old table replaced")
        
        # Check for duplicates
        total_rows = con.execute(f"SELECT COUNT(*) FROM {taxi}").fetchone()[0]
        distinct_rows = con.execute(f"SELECT COUNT(*) FROM (SELECT DISTINCT * FROM {taxi})").fetchone()[0]
        
        # The total invalid rows is the verification step that makes sure passenger_count, 
        # trip_distance, and the time interval is valid and satisfies the requirements for this project
        print(f"{taxi} - Invalid rows (2024): {invalid_count}")
        print(f"{taxi} - Total rows: {total_rows}")
        print(f"{taxi} - Distinct rows: {distinct_rows}")
        print("No duplicates found." if total_rows == distinct_rows else "Duplicates found.")
        
        logger.info(f"{taxi} - Invalid rows: {invalid_count}, Duplicates check: {'No duplicates' if total_rows == distinct_rows else 'Duplicates found'}")

if __name__ == "__main__":
    clean_and_verify_chunked()

