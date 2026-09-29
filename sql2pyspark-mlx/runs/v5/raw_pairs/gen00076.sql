SELECT
    session_id,
    event_type,
    SUM(CASE WHEN page = '/home' THEN duration_sec ELSE 0 END) AS home_duration_sec,
    APPROX_PERCENTILE(duration_sec, 0.5) AS duration_sec_p50,
    MAX(event_time) AS last_seen,
    APPROX_PERCENTILE(duration_sec, 0.5) AS duration_sec_p50_2,
    COUNT(DISTINCT event_type) AS distinct_event_types,
    STDDEV_SAMP(duration_sec) AS duration_sec_stddev,
    GROUPING_ID() AS grouping_id
FROM web_events
GROUP BY GROUPING SETS ((session_id, event_type), (session_id), ())
ORDER BY distinct_event_types DESC
