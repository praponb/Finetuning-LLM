WITH valid AS (
    -- Basic cleansing and defaults
    SELECT *, CEIL(temp_c) AS temp_c_whole
    FROM device_readings
    WHERE reading_ts > TO_DATE('2025-09-01')
),
ranked AS (
    -- QUALIFY-style window filter
    SELECT *, ROW_NUMBER() OVER (PARTITION BY device_code ORDER BY status_bits DESC, id) AS latest_rank
    FROM valid
    QUALIFY latest_rank = 1
),
with_gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN reading_ts - LAG(reading_ts) OVER (PARTITION BY device_code ORDER BY reading_ts, id) > INTERVAL '1' HOUR THEN 1 ELSE 0 END AS session_start_flag
    FROM ranked
),
sessions_device_readings AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(session_start_flag) OVER (PARTITION BY device_code ORDER BY reading_ts, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS island_id
    FROM with_gaps
),
ranked_device_readings AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY device_code ORDER BY reading_ts DESC, id) AS rank_in_group
        FROM sessions_device_readings
    ) t
    WHERE rank_in_group = 1
)
-- Multi-level aggregation
SELECT
    DATE_TRUNC('HOUR', reading_ts) AS hour_window,
    BIT_AND(status_bits) AS status_bits_bit_and,
    SIZE(COLLECT_SET(device_code)) AS num_device_codes,
    MAX(reading_ts) AS last_reading_ts,
    SUM(status_bits) AS total_status_bits,
    ROUND(AVG(humidity), 2) AS avg_humidity
FROM ranked_device_readings
GROUP BY hour_window
ORDER BY total_status_bits;
