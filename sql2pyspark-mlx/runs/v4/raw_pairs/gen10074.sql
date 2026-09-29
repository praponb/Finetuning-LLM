SELECT
    id,
    page,
    event_time,
    DENSE_RANK() OVER (PARTITION BY page ORDER BY event_time DESC) AS dense_rnk
FROM web_events
WHERE session_id IN (8, 33, 38, 39)
