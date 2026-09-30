SELECT
    DATE_TRUNC('YEAR', event_time) AS year_start,
    session_id,
    ROUND(AVG(duration_sec), 2) AS avg_duration_sec,
    BIT_AND(duration_sec) AS duration_sec_bit_and,
    MIN(event_time) AS first_event_time,
    PERCENTILE_APPROX(duration_sec, ARRAY(0.25, 0.5, 0.75)) AS duration_sec_quartiles,
    (UNIX_TIMESTAMP(MAX(event_time)) - UNIX_TIMESTAMP(MIN(event_time))) / 60 AS span_minutes,
    MAX(event_time) AS ended_at
FROM web_events
GROUP BY DATE_TRUNC('YEAR', event_time), session_id
ORDER BY duration_sec_bit_and ASC NULLS FIRST;
