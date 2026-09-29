WITH cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, pressure + temp_c AS pressure_plus_temp_c
    FROM device_readings
    WHERE HOUR(reading_ts) BETWEEN 11 AND 14
),
scored AS (
    -- Running and moving aggregates
    SELECT
        *,
        CORR(status_bits, humidity) OVER (PARTITION BY device_code) AS corr_status_bits_humidity,
        LAG(humidity, 1) OVER (PARTITION BY device_code ORDER BY temp_c, id) AS prev_humidity,
        ROW_NUMBER() OVER (ORDER BY temp_c, id) AS event_seq,
        ROUND(humidity / SUM(humidity) OVER (PARTITION BY device_code), 4) AS humidity_share
    FROM cleaned
),
deduped AS (
    -- Keep one row per key (latest first)
    SELECT *, ROW_NUMBER() OVER (PARTITION BY device_code ORDER BY event_seq DESC, id) AS latest_rank
    FROM scored
    QUALIFY latest_rank = 1
),
filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        humidity,
        pressure,
        status_bits,
        corr_status_bits_humidity,
        prev_humidity,
        humidity_share,
        latest_rank,
        DATEDIFF(CURRENT_DATE(), reading_ts) AS days_since_reading_ts
    FROM deduped
    WHERE reading_ts >= TIMESTAMP '2025-09-01 08:00:00'
),
filtered_device_readings AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        CAST(days_since_reading_ts AS DOUBLE) AS days_since_reading_ts_str,
        humidity_share + days_since_reading_ts AS humidity_share_plus_days_since_reading_ts
    FROM filtered
    WHERE days_since_reading_ts <= 57
)
-- Multi-level aggregation
SELECT
    CASE WHEN prev_humidity >= 77 THEN 'LARGE' ELSE 'SMALL' END AS prev_humidity_tier,
    CASE WHEN corr_status_bits_humidity >= 0.62 THEN 'PREMIUM' ELSE 'STANDARD' END AS corr_status_bits_humidity_tier,
    COUNT(*) AS row_count,
    PERCENTILE_APPROX(pressure, 0.9) AS pressure_p90
FROM filtered_device_readings
GROUP BY CASE WHEN prev_humidity >= 77 THEN 'LARGE' ELSE 'SMALL' END, CASE WHEN corr_status_bits_humidity >= 0.62 THEN 'PREMIUM' ELSE 'STANDARD' END
