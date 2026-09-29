SELECT
    DATE_TRUNC('MONTH', started_at) AS activity_month,
    (UNIX_TIMESTAMP(MAX(started_at)) - UNIX_TIMESTAMP(MIN(started_at))) / 60 AS duration_min,
    COUNT(CASE WHEN device = 'tablet' THEN 1 END) AS tablet_count,
    COUNT(*) FILTER (WHERE country = 'DE') AS de_rows,
    GROUPING(DATE_TRUNC('MONTH', started_at)) AS gid
FROM sessions
GROUP BY ROLLUP (DATE_TRUNC('MONTH', started_at))
ORDER BY duration_min DESC
