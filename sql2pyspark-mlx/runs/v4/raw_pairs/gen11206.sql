SELECT
    id,
    page,
    event_time,
    duration_sec,
    ROUND(AVG(duration_sec) OVER (PARTITION BY page ORDER BY event_time, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2) AS duration_sec_moving_avg_7
FROM web_events
WHERE event_time >= '2024-12-01';
