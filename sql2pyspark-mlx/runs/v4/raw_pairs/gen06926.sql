SELECT
    event_type,
    DATE_TRUNC('YEAR', event_time) AS year_start,
    COUNT(DISTINCT page) AS distinct_pages,
    ROUND(AVG(duration_sec), 2) AS avg_duration_sec,
    COUNT(DISTINCT session_id) AS distinct_session_ids,
    GROUPING_ID() AS grouping_level
FROM web_events
GROUP BY GROUPING SETS ((event_type, DATE_TRUNC('YEAR', event_time)), (event_type), ())
ORDER BY event_type NULLS LAST;
