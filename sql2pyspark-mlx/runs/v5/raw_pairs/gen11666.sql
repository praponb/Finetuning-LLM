WITH valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, UPPER(device_code) AS device_code_upper
    FROM device_readings
    WHERE reading_ts > TIMESTAMP '2025-09-01 02:00:00'
),
scored AS (
    -- Running and moving aggregates
    SELECT
        *,
        AVG(humidity) OVER w_grp AS humidity_avg_5,
        SUM(status_bits) OVER (PARTITION BY device_code ORDER BY temp_c, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_status_bits,
        SUM(humidity) OVER w_running AS device_code_sum_humidity
    FROM valid
    WINDOW w_grp AS (ORDER BY status_bits, id ROWS BETWEEN 5 PRECEDING AND CURRENT ROW), w_running AS (PARTITION BY device_code)
),
deduped AS (
    -- QUALIFY-style window filter
    SELECT
        id,
        device_code,
        reading_ts,
        humidity,
        pressure,
        status_bits,
        firmware,
        device_code_upper
    FROM scored
    QUALIFY ROW_NUMBER() OVER (PARTITION BY device_code ORDER BY humidity DESC, id) = 1
),
latest AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM deduped
    QUALIFY RANK() OVER (PARTITION BY device_code ORDER BY humidity DESC NULLS LAST) = 1
)
SELECT
    id,
    device_code,
    reading_ts,
    humidity,
    pressure,
    status_bits,
    firmware
FROM latest
