WITH gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN reading_ts - LAG(reading_ts) OVER (PARTITION BY device_code ORDER BY reading_ts, id) > INTERVAL 20 MINUTES THEN 1 ELSE 0 END AS is_new_burst
    FROM device_readings
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_burst) OVER (PARTITION BY device_code ORDER BY reading_ts, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM gaps
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
        is_new_burst,
        status_bits * is_new_burst AS status_bits_times_is_new_burst,
        CASE WHEN firmware IN ('1.0.3', '2.0.1') THEN 'Priority' ELSE 'Other' END AS firmware_group
    FROM sessionized
    WHERE reading_ts >= DATE '2025-09-01'
      AND firmware IN ('1.0.3', '2.0.1')
),
with_prev AS (
    -- Previous event time per key
    SELECT *, LAG(reading_ts) OVER (PARTITION BY device_code ORDER BY reading_ts, id) AS prev_reading_ts
    FROM valid
),
flagged AS (
    -- Flag rows that start a new session
    SELECT *, CASE WHEN prev_reading_ts IS NULL OR reading_ts > prev_reading_ts + INTERVAL 20 MINUTES THEN 1 ELSE 0 END AS new_group_flag
    FROM with_prev
),
sessionized_device_readings AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(new_group_flag) OVER (PARTITION BY device_code ORDER BY reading_ts, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS island_id
    FROM flagged
),
ranked AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM (
        SELECT *, RANK() OVER (PARTITION BY device_code ORDER BY prev_reading_ts NULLS LAST) AS rn
        FROM sessionized_device_readings
    ) t
    WHERE rn = 1
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        *,
        RANK() OVER (PARTITION BY device_code ORDER BY pressure) AS pressure_rank,
        PERCENT_RANK() OVER w_part AS new_group_flag_percent_rank,
        CORR(pressure, status_bits) OVER (PARTITION BY device_code) AS corr_pressure_status_bits
    FROM ranked
    WINDOW w_part AS (PARTITION BY device_code ORDER BY new_group_flag DESC)
)
-- Multi-level aggregation
SELECT
    DATE_TRUNC('DAY', reading_ts) AS activity_day,
    CASE WHEN status_bits >= 13 THEN 'PREMIUM' ELSE 'STANDARD' END AS status_bits_tier,
    COUNT(CASE WHEN device_code = 'DV-01' THEN 1 END) AS dv_01_count,
    COUNT_IF(device_code = 'DV-04') AS dv_04_events,
    SIZE(COLLECT_SET(device_code)) AS num_device_codes,
    COUNT(CASE WHEN device_code = 'DV-04' THEN 1 END) AS dv_04_count,
    ROUND(AVG(pressure), 2) AS avg_pressure,
    STDDEV_SAMP(humidity) AS humidity_stddev
FROM with_windows
GROUP BY DATE_TRUNC('DAY', reading_ts), CASE WHEN status_bits >= 13 THEN 'PREMIUM' ELSE 'STANDARD' END
ORDER BY avg_pressure NULLS FIRST;
