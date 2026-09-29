SELECT
    device,
    DATE_TRUNC('YEAR', started_at) AS year_start,
    DATE_TRUNC('QUARTER', started_at) AS quarter_start,
    COUNT(*) AS row_count,
    COUNT(*) AS event_count,
    MIN(started_at) AS first_started_at,
    (UNIX_TIMESTAMP(MAX(started_at)) - UNIX_TIMESTAMP(MIN(started_at))) / 60 AS duration_min,
    COUNT(*) FILTER (WHERE browser = 'Edge') AS edge_rows
FROM sessions
GROUP BY device, DATE_TRUNC('YEAR', started_at), DATE_TRUNC('QUARTER', started_at)
HAVING COUNT(*) FILTER (WHERE browser = 'Edge') >= 1;
