SELECT
    id,
    page,
    event_time,
    RANK() OVER (PARTITION BY page ORDER BY event_time) AS event_time_rank
FROM web_events;
