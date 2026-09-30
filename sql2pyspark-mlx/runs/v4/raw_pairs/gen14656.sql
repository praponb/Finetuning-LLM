SELECT
    id,
    page,
    duration_sec,
    SUM(duration_sec) OVER (PARTITION BY page) page_sum_duration_sec
FROM web_events
