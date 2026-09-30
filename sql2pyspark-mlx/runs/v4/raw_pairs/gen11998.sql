SELECT
    DATE_TRUNC('WEEK', started_at) AS week_start,
    COUNT(DISTINCT device) AS distinct_devices,
    SIZE(COLLECT_SET(browser)) AS num_browsers,
    MAX(started_at) AS last_started_at,
    COUNT(*) AS num_rows,
    COUNT_IF(device = 'desktop') AS desktop_events
FROM sessions
GROUP BY DATE_TRUNC('WEEK', started_at)
ORDER BY num_rows NULLS FIRST
