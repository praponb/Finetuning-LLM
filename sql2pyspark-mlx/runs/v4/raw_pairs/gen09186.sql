SELECT
    DATE_TRUNC('WEEK', event_time) AS week_start,
    DATE_TRUNC('QUARTER', event_time) AS quarter_start,
    ROUND(SUM(duration_sec) / NULLIF(COUNT(CASE WHEN page = '/search' THEN 1 END), 0), 2) AS duration_sec_per_search,
    STDDEV_SAMP(duration_sec) AS duration_sec_stddev,
    APPROX_PERCENTILE(duration_sec, ARRAY(0.25, 0.5, 0.75)) AS duration_sec_quartiles,
    (UNIX_TIMESTAMP(MAX(event_time)) - UNIX_TIMESTAMP(MIN(event_time))) / 60 AS span_minutes,
    ROUND(AVG(duration_sec), 2) AS avg_duration_sec
FROM web_events
GROUP BY DATE_TRUNC('WEEK', event_time), DATE_TRUNC('QUARTER', event_time)
HAVING avg_duration_sec > 0
ORDER BY quarter_start;
