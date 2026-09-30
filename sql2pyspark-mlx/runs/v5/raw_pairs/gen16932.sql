WITH latest AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM device_readings
    QUALIFY ROW_NUMBER() OVER (PARTITION BY device_code ORDER BY status_bits DESC, id) = 1
),
valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        INITCAP(TRIM(firmware)) AS firmware_title,
        FLOOR(humidity) AS humidity_whole
    FROM latest
    WHERE reading_ts >= TO_TIMESTAMP('2025-09-01 06:00:00')
),
valid_device_readings AS (
    -- Keep recent, valid rows
    SELECT *, CAST(reading_ts AS DATE) AS reading_ts_day
    FROM valid
    WHERE reading_ts >= DATE '2025-09-01'
      AND TO_DATE(reading_ts) < '2025-07-01'
),
deduped AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM valid_device_readings
    QUALIFY ROW_NUMBER() OVER (PARTITION BY device_code ORDER BY status_bits DESC, id) = 1
),
gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(reading_ts) OVER (PARTITION BY device_code ORDER BY reading_ts, id) IS NULL
                 OR reading_ts > LAG(reading_ts) OVER (PARTITION BY device_code ORDER BY reading_ts, id) + INTERVAL 30 MINUTES THEN 1 ELSE 0 END AS is_new_burst
    FROM deduped
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_burst) OVER (PARTITION BY device_code ORDER BY reading_ts, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM gaps
)
-- Grouped metrics
SELECT
    CASE WHEN burst_id >= 8 THEN 'PREMIUM' ELSE 'STANDARD' END AS burst_id_tier,
    CORR(pressure, temp_c) AS corr_pressure_temp_c,
    COUNT(*) FILTER (WHERE device_code = 'DV-01') AS dv_01_rows,
    SORT_ARRAY(COLLECT_SET(device_code)) AS device_code_list
FROM sessionized
GROUP BY 1
