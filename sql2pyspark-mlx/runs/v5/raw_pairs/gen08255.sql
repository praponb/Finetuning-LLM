SELECT
    DATE_TRUNC('QUARTER', started_at) AS quarter_start,
    browser,
    (UNIX_TIMESTAMP(MAX(started_at)) - UNIX_TIMESTAMP(MIN(started_at))) / 60 AS span_minutes,
    COUNT(CASE WHEN country = 'DE' THEN 1 END) AS de_count,
    COUNT(DISTINCT country) AS distinct_countries,
    MIN(started_at) AS first_started_at,
    GROUPING(DATE_TRUNC('QUARTER', started_at)) AS grouping_level
FROM sessions
GROUP BY CUBE (DATE_TRUNC('QUARTER', started_at), browser)
