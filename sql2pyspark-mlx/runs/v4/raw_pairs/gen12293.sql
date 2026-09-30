WITH enriched AS (
    SELECT
        *,
        LAST_VALUE(temp_c, TRUE) OVER (ORDER BY reading_ts, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_temp_c,
        CORR(humidity, pressure) OVER (PARTITION BY device_code) AS corr_humidity_pressure
    FROM device_readings
),
deduped AS (
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY device_code ORDER BY reading_ts, id) AS latest_rank
        FROM enriched
    ) ranked
    WHERE latest_rank = 1
),
flagged AS (
    SELECT *, CASE WHEN LAG(reading_ts) OVER (PARTITION BY device_code ORDER BY reading_ts, id) IS NULL
                 OR reading_ts > LAG(reading_ts) OVER (PARTITION BY device_code ORDER BY reading_ts, id) + INTERVAL '20' MINUTE THEN 1 ELSE 0 END AS session_start_flag
    FROM deduped
),
sessions_device_readings AS (
    SELECT *, SUM(session_start_flag) OVER (PARTITION BY device_code ORDER BY reading_ts, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS island_id
    FROM flagged
),
latest AS (
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY device_code ORDER BY status_bits DESC, id) AS dedup_rank
        FROM sessions_device_readings
    ) ranked
    WHERE dedup_rank = 1
),
flagged_2 AS (
    SELECT *, CASE WHEN reading_ts - LAG(reading_ts) OVER (PARTITION BY device_code ORDER BY reading_ts, id) > INTERVAL 30 MINUTES THEN 1 ELSE 0 END AS session_start_flag_2
    FROM latest
),
sessionized AS (
    SELECT *, SUM(session_start_flag_2) OVER (PARTITION BY device_code ORDER BY reading_ts, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM flagged_2
)
SELECT
    DATE_TRUNC('DAY', reading_ts) AS activity_day,
    DATE_TRUNC('HOUR', reading_ts) AS hour_window,
    ROUND(AVG(island_id), 2) AS avg_island_id,
    MAX(reading_ts) AS last_reading_ts,
    COUNT_IF(device_code = 'DV-01') AS dv_01_events,
    MAX(reading_ts) AS last_seen,
    (UNIX_TIMESTAMP(MAX(reading_ts)) - UNIX_TIMESTAMP(MIN(reading_ts))) / 60 AS span_minutes,
    SORT_ARRAY(COLLECT_SET(device_code)) AS device_code_list
FROM sessionized
GROUP BY DATE_TRUNC('DAY', reading_ts), DATE_TRUNC('HOUR', reading_ts)
ORDER BY hour_window
