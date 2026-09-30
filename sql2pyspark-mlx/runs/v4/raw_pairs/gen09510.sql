WITH valid AS (
    -- Basic cleansing and defaults
    SELECT *, COALESCE(temp_c, 0) AS temp_c_filled
    FROM device_readings
    WHERE firmware IN ('2.0.1', '1.1.0', '1.0.3')
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        device_code,
        reading_ts,
        temp_c,
        humidity,
        pressure,
        status_bits,
        firmware,
        temp_c_filled,
        LEAD(status_bits, 1) OVER (ORDER BY status_bits, id) AS next_status_bits,
        SUM(humidity) OVER (PARTITION BY device_code) AS device_code_sum_humidity
    FROM valid
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        AVG(temp_c) OVER (PARTITION BY device_code ORDER BY CAST(reading_ts AS BIGINT) RANGE BETWEEN 1800 PRECEDING AND CURRENT ROW) AS temp_c_avg_30m,
        FIRST_VALUE(temp_c) IGNORE NULLS OVER (PARTITION BY device_code ORDER BY temp_c, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_temp_c,
        SUM(status_bits) OVER (PARTITION BY device_code ORDER BY temp_c, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_status_bits
    FROM metrics
)
SELECT
    id,
    device_code,
    reading_ts,
    temp_c,
    humidity,
    pressure,
    status_bits,
    firmware,
    next_status_bits
FROM enriched
ORDER BY firmware DESC NULLS FIRST;
