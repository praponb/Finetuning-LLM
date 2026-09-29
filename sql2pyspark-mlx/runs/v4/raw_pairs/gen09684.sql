WITH valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        device_code,
        reading_ts,
        temp_c,
        humidity,
        pressure,
        status_bits,
        firmware,
        temp_c - status_bits AS temp_c_minus_status_bits
    FROM device_readings
    WHERE reading_ts >= TIMESTAMP '2025-09-01 08:00:00'
),
deduped AS (
    -- QUALIFY-style window filter
    SELECT *, RANK() OVER (PARTITION BY device_code ORDER BY humidity DESC NULLS FIRST) AS latest_rank
    FROM valid
    QUALIFY latest_rank = 1
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        id,
        device_code,
        reading_ts,
        temp_c,
        humidity,
        pressure,
        status_bits,
        firmware,
        temp_c_minus_status_bits,
        latest_rank,
        CORR(humidity, status_bits) OVER (PARTITION BY device_code) AS corr_humidity_status_bits,
        ROUND(pressure / SUM(pressure) OVER (PARTITION BY device_code), 4) AS pressure_share,
        SUM(temp_c_minus_status_bits) OVER (PARTITION BY device_code ORDER BY CAST(reading_ts AS BIGINT) RANGE BETWEEN 3600 PRECEDING AND CURRENT ROW) AS temp_c_minus_status_bits_sum_1h
    FROM deduped
),
deduped_device_readings AS (
    -- QUALIFY-style window filter
    SELECT *, ROW_NUMBER() OVER (PARTITION BY device_code ORDER BY temp_c_minus_status_bits DESC, id) AS rn
    FROM metrics
    QUALIFY rn = 1
)
SELECT
    id,
    reading_ts,
    temp_c,
    pressure,
    firmware,
    temp_c_minus_status_bits,
    latest_rank,
    corr_humidity_status_bits,
    pressure_share
FROM deduped_device_readings;
