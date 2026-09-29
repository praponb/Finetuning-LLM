WITH scored AS (
    -- Ranking and gap features
    SELECT
        *,
        LAG(status_bits, 1) OVER (PARTITION BY device_code ORDER BY reading_ts, id) AS prev_status_bits,
        LAST_VALUE(pressure, TRUE) OVER (PARTITION BY device_code ORDER BY reading_ts, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_pressure,
        SUM(pressure) OVER (PARTITION BY device_code ORDER BY status_bits, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_pressure,
        ROW_NUMBER() OVER (ORDER BY reading_ts DESC, id) AS seq_no
    FROM device_readings
    WHERE reading_ts < '2024-08-01'
),
valid AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        device_code,
        reading_ts,
        humidity,
        pressure,
        status_bits,
        firmware,
        prev_status_bits,
        last_known_pressure,
        running_pressure,
        LENGTH(firmware) AS firmware_length
    FROM scored
    WHERE reading_ts >= TIMESTAMP '2025-09-01 03:00:00'
      AND TO_DATE(reading_ts) >= '2023-12-01'
),
with_gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN reading_ts - LAG(reading_ts) OVER (PARTITION BY device_code ORDER BY reading_ts, id) > INTERVAL 20 MINUTES THEN 1 ELSE 0 END AS session_start_flag
    FROM valid
),
islands AS (
    -- Running sum of the flag = session number
    SELECT
        id,
        device_code,
        reading_ts,
        firmware,
        firmware_length,
        SUM(session_start_flag) OVER (PARTITION BY device_code ORDER BY reading_ts, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS island_id
    FROM with_gaps
),
latest AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY device_code ORDER BY island_id DESC, id) AS dedup_rank
        FROM islands
    ) ranked
    WHERE dedup_rank = 1
)
-- Multi-level aggregation
SELECT
    island_id,
    SORT_ARRAY(COLLECT_SET(device_code)) AS device_code_list,
    SORT_ARRAY(COLLECT_SET(device_code)) AS device_code_list_2,
    SUM(firmware_length) AS total_firmware_length
FROM latest
GROUP BY island_id
HAVING SUM(firmware_length) > 0
ORDER BY island_id
