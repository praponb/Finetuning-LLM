SELECT
    DATE_TRUNC('MONTH', event_time) AS event_month,
    event_type,
    SUM(duration_sec) AS total_duration_sec,
    SUM(CASE WHEN page = '/checkout' THEN duration_sec ELSE 0 END) AS checkout_duration_sec,
    ROUND(STDDEV(duration_sec), 2) AS stddev_duration_sec
FROM web_events
GROUP BY DATE_TRUNC('MONTH', event_time), event_type
HAVING checkout_duration_sec > 23;
