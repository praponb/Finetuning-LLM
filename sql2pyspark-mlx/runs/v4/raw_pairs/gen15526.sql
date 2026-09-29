SELECT
    id,
    page,
    duration_sec,
    MAX(duration_sec) OVER (PARTITION BY page) AS page_max_duration_sec
FROM web_events
