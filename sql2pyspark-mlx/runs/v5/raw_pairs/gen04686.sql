SELECT
    DATE_TRUNC('QUARTER', started_at) AS quarter_start,
    COUNT_IF(country = 'DE') AS de_events,
    COUNT_IF(country = 'SG') AS sg_events
FROM sessions
GROUP BY DATE_TRUNC('QUARTER', started_at)
