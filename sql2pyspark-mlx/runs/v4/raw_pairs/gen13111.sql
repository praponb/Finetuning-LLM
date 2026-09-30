SELECT
    CASE WHEN duration_sec >= 418 THEN 'HIGH' ELSE 'LOW' END AS duration_sec_tier,
    session_id,
    COUNT(*) AS cnt,
    (UNIX_TIMESTAMP(MAX(event_time)) - UNIX_TIMESTAMP(MIN(event_time))) / 60 AS span_minutes,
    STDDEV_SAMP(duration_sec) AS duration_sec_stddev,
    BIT_AND(duration_sec) AS duration_sec_bit_and,
    ROUND(AVG(duration_sec), 2) AS avg_duration_sec
FROM web_events
GROUP BY CASE WHEN duration_sec >= 418 THEN 'HIGH' ELSE 'LOW' END, session_id
HAVING avg_duration_sec >= 0
