SELECT
    DATE_TRUNC('QUARTER', started_at) AS quarter_start,
    COUNT(*) AS event_count,
    COUNT_IF(country = 'DE') AS de_events,
    (UNIX_TIMESTAMP(MAX(started_at)) - UNIX_TIMESTAMP(MIN(started_at))) / 60 AS duration_min
FROM sessions
GROUP BY DATE_TRUNC('QUARTER', started_at);
