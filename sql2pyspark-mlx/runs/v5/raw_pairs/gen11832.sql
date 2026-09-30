WITH cleaned AS (
    SELECT *, ROUND(temp_c, 2) AS temp_c_rounded
    FROM device_readings
    WHERE HOUR(reading_ts) BETWEEN 6 AND 11
),
with_prev AS (
    SELECT *, LAG(reading_ts) OVER (PARTITION BY device_code ORDER BY reading_ts, id) AS prev_reading_ts
    FROM cleaned
),
gaps AS (
    SELECT *, CASE WHEN prev_reading_ts IS NULL OR reading_ts > prev_reading_ts + INTERVAL 20 MINUTES THEN 1 ELSE 0 END AS is_new_session
    FROM with_prev
),
sessions_device_readings AS (
    SELECT
        id,
        device_code,
        reading_ts,
        humidity,
        status_bits,
        SUM(is_new_session) OVER (PARTITION BY device_code ORDER BY reading_ts, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM gaps
),
enriched AS (
    SELECT
        *,
        COUNT(session_no) OVER w_ordered AS running_count_session_no,
        AVG(session_no) OVER w_ordered AS running_avg_session_no,
        MIN(session_no) OVER w_ordered AS running_min_session_no
    FROM sessions_device_readings
    WINDOW w_ordered AS (PARTITION BY device_code ORDER BY humidity, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
prev_events AS (
    SELECT *, LAG(reading_ts) OVER (PARTITION BY device_code ORDER BY reading_ts, id) AS prev_reading_ts
    FROM enriched
),
with_gaps AS (
    SELECT *, CASE WHEN prev_reading_ts IS NULL OR reading_ts > prev_reading_ts + INTERVAL '15' MINUTE THEN 1 ELSE 0 END AS new_group_flag
    FROM prev_events
),
sessionized AS (
    SELECT
        id,
        device_code,
        reading_ts,
        humidity,
        status_bits,
        session_no,
        SUM(new_group_flag) OVER (PARTITION BY device_code ORDER BY reading_ts, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM with_gaps
),
filtered AS (
    SELECT
        id,
        reading_ts,
        humidity,
        session_no,
        CAST(reading_ts AS DATE) AS reading_ts_day,
        CASE WHEN session_id >= 7 THEN 'High' WHEN session_id >= 3 THEN 'Medium' ELSE 'Low' END AS session_id_level
    FROM sessionized
    WHERE reading_ts > TO_TIMESTAMP('2025-09-01 07:00:00')
      AND session_no >= 4
)
SELECT
    id,
    reading_ts,
    humidity,
    session_no,
    reading_ts_day,
    session_id_level
FROM filtered
ORDER BY humidity DESC
