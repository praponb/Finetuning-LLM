SELECT
    DATE_TRUNC('QUARTER', started_at) AS quarter_start,
    DATE_TRUNC('WEEK', started_at) AS week_start,
    SIZE(COLLECT_SET(device)) AS num_devices,
    (UNIX_TIMESTAMP(MAX(started_at)) - UNIX_TIMESTAMP(MIN(started_at))) / 60 AS duration_min,
    (UNIX_TIMESTAMP(MAX(started_at)) - UNIX_TIMESTAMP(MIN(started_at))) / 60 AS span_minutes,
    COUNT_IF(country = 'TH') AS th_events,
    COUNT(CASE WHEN device = 'mobile' THEN 1 END) AS mobile_count
FROM sessions
GROUP BY DATE_TRUNC('QUARTER', started_at), DATE_TRUNC('WEEK', started_at)
HAVING SIZE(COLLECT_SET(device)) > 1;
