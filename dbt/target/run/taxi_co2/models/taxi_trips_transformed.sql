
  
    
    

    create  table
      "emissions"."main"."taxi_trips_transformed__dbt_tmp"
  
    as (
      WITH yellow_trips AS (
    SELECT
        'yellow' AS taxi_type,
        t.pick_up_datetime,
        t.drop_off_dt,
        t.passenger_count,
        t.trip_distance,
        EXTRACT(EPOCH FROM (t.drop_off_dt - t.pick_up_datetime)) AS trip_duration_seconds,
        t.trip_distance / (EXTRACT(EPOCH FROM (t.drop_off_dt - t.pick_up_datetime)) / 3600) AS avg_mph,
        (t.trip_distance * v.co2_grams_per_mile)/1000 AS co2_kg_per_trip,
        EXTRACT(HOUR FROM t.pick_up_datetime) AS trip_hour,
        EXTRACT(DOW FROM t.pick_up_datetime) AS trip_day_of_week,
        EXTRACT(WEEK FROM t.pick_up_datetime) AS trip_week,
        EXTRACT(MONTH FROM t.pick_up_datetime) AS trip_month
    FROM "emissions"."main"."yellow_taxi_data" t
    LEFT JOIN "emissions"."main"."vehicle_emissions" v
        ON v.vehicle_type = 'yellow_taxi'
    WHERE t.drop_off_dt > t.pick_up_datetime
),

green_trips AS (
    SELECT
        'green' AS taxi_type,
        t.pick_up_datetime,
        t.drop_off_dt,
        t.passenger_count,
        t.trip_distance,
        EXTRACT(EPOCH FROM (t.drop_off_dt - t.pick_up_datetime)) AS trip_duration_seconds,
        t.trip_distance / (EXTRACT(EPOCH FROM (t.drop_off_dt - t.pick_up_datetime)) / 3600) AS avg_mph,
        (t.trip_distance * v.co2_grams_per_mile)/1000 AS co2_kg_per_trip,
        EXTRACT(HOUR FROM t.pick_up_datetime) AS trip_hour,
        EXTRACT(DOW FROM t.pick_up_datetime) AS trip_day_of_week,
        EXTRACT(WEEK FROM t.pick_up_datetime) AS trip_week,
        EXTRACT(MONTH FROM t.pick_up_datetime) AS trip_month
    FROM "emissions"."main"."green_taxi_data" t
    LEFT JOIN "emissions"."main"."vehicle_emissions" v
        ON v.vehicle_type = 'green_taxi'
    WHERE t.drop_off_dt > t.pick_up_datetime
)

SELECT * FROM yellow_trips
UNION ALL
SELECT * FROM green_trips
    );
  
  