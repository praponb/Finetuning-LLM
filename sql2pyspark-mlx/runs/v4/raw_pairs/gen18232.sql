SELECT
    id,
    page,
    duration_sec,
    AVG(duration_sec) OVER (PARTITION BY page) AS page_avg_duration_sec
FROM web_events
WHERE page = '/checkout'
