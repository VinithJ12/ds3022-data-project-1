
  
    
    

    create  table
      "emissions"."main"."nyc_taxi_emissions__dbt_tmp"
  
    as (
      

SELECT
    'yellow' AS taxi_type,
    t.pick_up_datetime,
    t.drop_off_dt,
    t.passenger_count,
    t.trip_distance,
    EXTRACT(EPOCH FROM (t.drop_off_dt - t.pick_up_datetime)) AS trip_duration_seconds,
    (t.trip_distance / (EXTRACT(EPOCH FROM (t.drop_off_dt - t.pick_up_datetime))/3600)) AS avg_mph,
    (t.trip_distance * v.co2_g_per_mile)/1000 AS co2_kg_per_trip,
    EXTRACT(HOUR FROM t.pick_up_datetime) AS trip_hour,
    EXTRACT(DOW FROM t.pick_up_datetime) AS trip_day_of_week,
    EXTRACT(WEEK FROM t.pick_up_datetime) AS trip_week,
    EXTRACT(MONTH FROM t.pick_up_datetime) AS trip_month
FROM "emissions"."nyc_taxi"."yellow_taxi_data" t
LEFT JOIN "emissions"."nyc_taxi"."vehicle_emissions" v
    ON 'yellow' = v.vehicle_type

UNION ALL

SELECT
    'green' AS taxi_type,
    t.pick_up_datetime,
    t.drop_off_dt,
    t.passenger_count,
    t.trip_distance,
    EXTRACT(EPOCH FROM (t.drop_off_dt - t.pick_up_datetime)) AS trip_duration_seconds,
    (t.trip_distance / (EXTRACT(EPOCH FROM (t.drop_off_dt - t.pick_up_datetime))/3600)) AS avg_mph,
    (t.trip_distance * v.co2_g_per_mile)/1000 AS co2_kg_per_trip,
    EXTRACT(HOUR FROM t.pick_up_datetime) AS trip_hour,
    EXTRACT(DOW FROM t.pick_up_datetime) AS trip_day_of_week,
    EXTRACT(WEEK FROM t.pick_up_datetime) AS trip_week,
    EXTRACT(MONTH FROM t.pick_up_datetime) AS trip_month
FROM "emissions"."nyc_taxi"."green_taxi_data" t
LEFT JOIN "emissions"."nyc_taxi"."vehicle_emissions" v
    ON 'green' = v.vehicle_type
    );
  
  