PRAGMA threads=4;
PRAGMA enable_progress_bar;
PRAGMA memory_limit='4GB';

CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;

-- Load bronze layer from locally cached parquet files.
CREATE OR REPLACE TABLE bronze.yellow_tripdata AS
SELECT *
FROM read_parquet('data/raw/yellow_tripdata_*.parquet');

-- Reference data for taxi zones.
CREATE OR REPLACE TABLE silver.taxi_zones AS
SELECT
    CAST(LocationID AS INTEGER)      AS location_id,
    TRIM(Borough)                    AS borough,
    TRIM(Zone)                       AS zone,
    TRIM(service_zone)               AS service_zone
FROM read_csv_auto('data/reference/taxi_zone_lookup.csv', header=true);

-- Silver layer with cleansing + engineered metrics.
CREATE OR REPLACE TABLE silver.yellow_tripdata_clean AS
WITH base AS (
    SELECT
        VendorID,
        tpep_pickup_datetime   AS pickup_ts,
        tpep_dropoff_datetime  AS dropoff_ts,
        passenger_count,
        trip_distance,
        RatecodeID,
        store_and_fwd_flag,
        PULocationID,
        DOLocationID,
        payment_type,
        fare_amount,
        extra,
        mta_tax,
        tip_amount,
        tolls_amount,
        improvement_surcharge,
        total_amount,
        congestion_surcharge,
        airport_fee,
        coalesce(cbd_congestion_fee, 0) AS cbd_congestion_fee,
        datediff('minute', tpep_pickup_datetime, tpep_dropoff_datetime) AS trip_minutes,
        fare_amount + tip_amount + tolls_amount + congestion_surcharge + coalesce(cbd_congestion_fee,0) AS gross_revenue,
        fare_amount / NULLIF(trip_distance,0) AS fare_per_mile
    FROM bronze.yellow_tripdata
)
SELECT
    *,
    CASE WHEN trip_distance <= 0 OR fare_amount <= 0 OR trip_minutes NOT BETWEEN 1 AND 240 OR passenger_count NOT BETWEEN 1 AND 6 THEN TRUE ELSE FALSE END AS invalid_trip_flag
FROM base
WHERE trip_distance > 0
  AND fare_amount > 0
  AND passenger_count BETWEEN 1 AND 6
  AND trip_minutes BETWEEN 1 AND 240
  AND PULocationID IS NOT NULL
  AND DOLocationID IS NOT NULL;

-- Data quality summary table for transparency.
CREATE OR REPLACE TABLE silver.data_quality_summary AS
SELECT
    CURRENT_TIMESTAMP AS generated_at,
    COUNT(*) AS bronze_rows,
    SUM(
        CASE
            WHEN trip_distance <= 0
              OR fare_amount <= 0
              OR passenger_count NOT BETWEEN 1 AND 6
              OR datediff('minute', tpep_pickup_datetime, tpep_dropoff_datetime) NOT BETWEEN 1 AND 240
              OR PULocationID IS NULL
              OR DOLocationID IS NULL
            THEN 1 ELSE 0
        END
    ) AS invalid_bronze_rows,
    (SELECT COUNT(*) FROM silver.yellow_tripdata_clean) AS silver_rows
FROM bronze.yellow_tripdata;

-- Gold layer 1: zone-hourly aggregates with zone lookup join.
CREATE OR REPLACE TABLE gold.zone_hourly_metrics AS
SELECT
    date_trunc('hour', pickup_ts)                 AS pickup_hour,
    z.borough                                     AS pickup_borough,
    z.zone                                        AS pickup_zone,
    COUNT(*)                                      AS trips,
    SUM(passenger_count)                          AS passengers,
    SUM(trip_distance)                            AS total_miles,
    AVG(trip_distance)                            AS avg_miles,
    SUM(gross_revenue)                            AS gross_revenue,
    SUM(cbd_congestion_fee)                       AS congestion_fees,
    SUM(tip_amount)                               AS tips,
    SUM(CASE WHEN RatecodeID = 2 THEN 1 ELSE 0 END) AS jfk_trips
FROM silver.yellow_tripdata_clean s
JOIN silver.taxi_zones z ON s.PULocationID = z.location_id
GROUP BY 1,2,3;

CREATE OR REPLACE TABLE gold.payment_type_metrics AS
WITH payment_dim(payment_type, description) AS (
    VALUES
        (1, 'Credit Card'),
        (2, 'Cash'),
        (3, 'No Charge'),
        (4, 'Dispute'),
        (5, 'Unknown'),
        (6, 'Voided')
)
SELECT
    date_trunc('day', pickup_ts) AS service_day,
    p.description,
    COUNT(*)                     AS trips,
    SUM(gross_revenue)           AS revenue,
    AVG(fare_per_mile)           AS fare_per_mile
FROM silver.yellow_tripdata_clean s
LEFT JOIN payment_dim p USING (payment_type)
GROUP BY 1,2;

-- Optional view to simplify BI connections.
CREATE OR REPLACE VIEW gold.latest_hourly_view AS
SELECT *
FROM gold.zone_hourly_metrics
WHERE pickup_hour >= (SELECT max(pickup_hour) - INTERVAL '7 days' FROM gold.zone_hourly_metrics);
