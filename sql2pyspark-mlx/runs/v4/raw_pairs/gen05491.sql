WITH filtered AS (
    SELECT
        id,
        device_code,
        reading_ts,
        pressure,
        status_bits,
        firmware,
        COALESCE(humidity, 0) AS humidity_filled,
        status_bits + humidity AS status_bits_plus_humidity
    FROM device_readings
    WHERE reading_ts >= DATE '2025-09-01'
),
gaps AS (
    SELECT *, CASE WHEN reading_ts - LAG(reading_ts) OVER (PARTITION BY device_code ORDER BY reading_ts, id) > INTERVAL '1' HOUR THEN 1 ELSE 0 END AS session_start_flag
    FROM filtered
),
sessionized AS (
    SELECT *, SUM(session_start_flag) OVER (PARTITION BY device_code ORDER BY reading_ts, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM gaps
),
with_gaps AS (
    SELECT *, CASE WHEN reading_ts - LAG(reading_ts) OVER (PARTITION BY device_code ORDER BY reading_ts, id) > INTERVAL 30 MINUTES THEN 1 ELSE 0 END AS is_new_session
    FROM sessionized
),
islands AS (
    SELECT *, SUM(is_new_session) OVER (PARTITION BY device_code ORDER BY reading_ts, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM with_gaps
)
SELECT
    CASE WHEN session_id >= 7 THEN 'PREMIUM' ELSE 'STANDARD' END AS session_id_tier,
    device_code,
    SUM(CASE WHEN device_code = 'DV-03' THEN is_new_session ELSE 0 END) AS dv_03_is_new_session,
    MAX(reading_ts) AS ended_at,
    MIN(burst_id) AS min_burst_id
FROM islands
GROUP BY ROLLUP (CASE WHEN session_id >= 7 THEN 'PREMIUM' ELSE 'STANDARD' END, device_code)
ORDER BY min_burst_id NULLS LAST;
