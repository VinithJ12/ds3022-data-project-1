import duckdb
import logging
import matplotlib.pyplot as plt

#DISCLAIMER: # Execute the SQL query and fetch the results as a pandas DataFrame
# This allows us to manipulate, print, and plot the data easily with pandas and matplotlib


# Logging setup
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    filename='analysis.log',
    filemode='w'
)
logger = logging.getLogger(__name__)


#LET'S make a function that does some analysis of our now transformed and cleaned data
def analyze_taxi_data():
    con = duckdb.connect(database='emissions.duckdb', read_only=True)
    logger.info("Connected to emissions.duckdb for analysis")

    try:
        # Largest carbon-producing trip for each taxi type
        print("\nLargest carbon producing trip per taxi type")
        largest_trip = con.execute("""
            SELECT taxi_type, MAX(co2_kg_per_trip) AS max_co2
            FROM taxi_trips_transformed
            GROUP BY taxi_type
        """).fetchdf()
        print(largest_trip)

        logger.info(f"Largest CO2 trip:\n{largest_trip}")

        # Most and least carbon-heavy hours of the day
        print("\n Most and least carbon heavy hours per taxi type")
        hours = con.execute("""
            SELECT taxi_type,
                   MAX(avg_co2) AS max_hour_avg_co2,
                   MIN(avg_co2) AS min_hour_avg_co2
            FROM (
                SELECT taxi_type, trip_hour, AVG(co2_kg_per_trip) AS avg_co2
                FROM taxi_trips_transformed
                GROUP BY taxi_type, trip_hour
            )
            GROUP BY taxi_type
        """).fetchdf()
        print(hours)

        logger.info(f"CO2-heavy/light hours:\n{hours}")

        # Most and least carbon-heavy days of the week
        print("\nMost and least carbon heavy days per taxi type")
        days = con.execute("""
            SELECT taxi_type,
                   MAX(avg_co2) AS max_day_avg_co2,
                   MIN(avg_co2) AS min_day_avg_co2
            FROM (
                SELECT taxi_type, trip_day_of_week, AVG(co2_kg_per_trip) AS avg_co2
                FROM taxi_trips_transformed
                GROUP BY taxi_type, trip_day_of_week
            )
            GROUP BY taxi_type
        """).fetchdf()
        print(days)

        logger.info(f"CO2-heavy/light days:\n{days}")

        # Most and least carbon-heavy weeks of the year (2024)
        print("\nMost and least carbon heavy weeks per taxi type")
        weeks = con.execute("""
            SELECT taxi_type,
                   MAX(avg_co2) AS max_week_avg_co2,
                   MIN(avg_co2) AS min_week_avg_co2
            FROM (
                SELECT taxi_type, trip_week, AVG(co2_kg_per_trip) AS avg_co2
                FROM taxi_trips_transformed
                GROUP BY taxi_type, trip_week
            )
            GROUP BY taxi_type
        """).fetchdf()
        print(weeks)

        logger.info(f"CO2-heavy/light weeks:\n{weeks}")

        # Most and least carbon-heavy months of the year
        print("\nMost and least carbon heavy months per taxi type")
        months = con.execute("""
            SELECT taxi_type,
                   MAX(avg_co2) AS max_month_avg_co2,
                   MIN(avg_co2) AS min_month_avg_co2
            FROM (
                SELECT taxi_type, trip_month, AVG(co2_kg_per_trip) AS avg_co2
                FROM taxi_trips_transformed
                GROUP BY taxi_type, trip_month
            )
            GROUP BY taxi_type
        """).fetchdf()
        print(months)
    
        logger.info(f"CO2-heavy/light months:\n{months}")

        print("\n Plotting total monthly CO2 per taxi type")
        monthly_totals = con.execute("""
            SELECT trip_month, taxi_type, SUM(co2_kg_per_trip) AS total_co2
            FROM taxi_trips_transformed
            GROUP BY trip_month, taxi_type
            ORDER BY trip_month
        """).fetchdf()

        plt.figure(figsize=(10,6))
        colors= {'yellow': 'gold', 'green': 'green'}
        for taxi_type in ['yellow', 'green']:
            subset = monthly_totals[monthly_totals['taxi_type'] == taxi_type]
            plt.plot(subset['trip_month'], subset['total_co2'], marker='o', label=taxi_type.capitalize(), color= colors[taxi_type])
            max_idx = subset['total_co2'].idxmax()
            max_month = subset.loc[max_idx, 'trip_month']
            max_co2 = subset.loc[max_idx, 'total_co2']
            plt.text(max_month, max_co2, f'{max_co2:.0f}', ha='center', va='bottom', color=colors[taxi_type], fontweight='bold')
            
        plt.title("Total Monthly CO2 by Taxi Type")
        plt.yscale('log')
        plt.xlabel("Month")
        plt.ylabel("Total CO2 (kg)")
        plt.xticks(range(1,13))
        plt.legend()
        plt.grid(True)
        plt.tight_layout()
        plt.savefig("monthly_co2_trend.png")
        print("Plot saved as 'monthly_co2_trend.png'")
        logger.info("Plot saved as 'monthly_co2_trend.png'")

    except Exception as e:
        logger.error(f"Error during analysis: {e}")
        print("Error during analysis:", e)

    finally:
        con.close()
        logger.info("DuckDB connection closed")

if __name__ == "__main__":
    analyze_taxi_data()

