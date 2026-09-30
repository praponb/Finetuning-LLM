WITH base AS (
    SELECT
        *,
        ROUND(pressure, 0) AS pressure_rounded,
        UPPER(device_code) AS device_code_upper
    FROM device_readings
    WHERE firmware NOT IN ('2.0.1', '1.1.0')
),
valid AS (
    SELECT *, INITCAP(TRIM(firmware)) AS firmware_title
    FROM base
    WHERE reading_ts > TO_DATE('2025-09-01')
),
latest AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY device_code ORDER BY pressure DESC, id) AS row_num
    FROM valid
    QUALIFY row_num = 1
),
prev_events AS (
    SELECT *, LAG(reading_ts) OVER (PARTITION BY device_code ORDER BY reading_ts, id) AS prev_reading_ts
    FROM latest
),
gaps AS (
    SELECT *, CASE WHEN prev_reading_ts IS NULL OR reading_ts > prev_reading_ts + INTERVAL 45 MINUTES THEN 1 ELSE 0 END AS is_new_burst
    FROM prev_events
),
sessions_device_readings AS (
    SELECT *, SUM(is_new_burst) OVER (PARTITION BY device_code ORDER BY reading_ts, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM gaps
),
with_gaps AS (
    SELECT *, CASE WHEN LAG(reading_ts) OVER (PARTITION BY device_code ORDER BY reading_ts, id) IS NULL
                 OR reading_ts > LAG(reading_ts) OVER (PARTITION BY device_code ORDER BY reading_ts, id) + INTERVAL '45' MINUTE THEN 1 ELSE 0 END AS is_new_session
    FROM sessions_device_readings
),
sessions_device_readings_2 AS (
    SELECT *, SUM(is_new_session) OVER (PARTITION BY device_code ORDER BY reading_ts, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM with_gaps
),
latest_device_readings AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY device_code ORDER BY pressure_rounded DESC NULLS LAST, id) AS rn
    FROM sessions_device_readings_2
    QUALIFY rn = 1
),
flagged AS (
    SELECT *, CASE WHEN LAG(prev_reading_ts) OVER (PARTITION BY device_code ORDER BY prev_reading_ts, id) IS NULL
                 OR prev_reading_ts > LAG(prev_reading_ts) OVER (PARTITION BY device_code ORDER BY prev_reading_ts, id) + INTERVAL '30' DAY THEN 1 ELSE 0 END AS is_new_burst_2
    FROM latest_device_readings
),
islands AS (
    SELECT *, SUM(is_new_burst_2) OVER (PARTITION BY device_code ORDER BY prev_reading_ts, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id_2
    FROM flagged
)
SELECT
    CASE WHEN session_id >= 9 THEN 'LARGE' ELSE 'SMALL' END AS session_id_tier,
    CASE WHEN humidity >= 80 THEN 'HIGH' ELSE 'LOW' END AS humidity_tier,
    CASE WHEN is_new_burst >= 1 THEN 'HIGH' ELSE 'LOW' END AS is_new_burst_tier,
    PERCENTILE_APPROX(session_id, 0.95) AS session_id_p95,
    MAX(session_no) AS max_session_no,
    ROUND(SUM(status_bits) / NULLIF(COUNT(CASE WHEN device_code = 'DV-03' THEN 1 END), 0), 2) AS status_bits_per_dv_03,
    COUNT_IF(device_code = 'DV-01') AS dv_01_events,
    ROUND(SUM(pressure) / NULLIF(COUNT(CASE WHEN device_code = 'DV-01' THEN 1 END), 0), 2) AS pressure_per_dv_01,
    COUNT(DISTINCT device_code) AS distinct_device_codes,
    DENSE_RANK() OVER (PARTITION BY CASE WHEN session_id >= 9 THEN 'LARGE' ELSE 'SMALL' END ORDER BY PERCENTILE_APPROX(session_id, 0.95) DESC) AS session_id_p95_dense_rank
FROM islands
GROUP BY session_id_tier, humidity_tier, is_new_burst_tier
ORDER BY humidity_tier DESC NULLS FIRST
