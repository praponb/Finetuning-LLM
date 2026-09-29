SELECT
    id,
    page,
    event_time,
    event_type,
    DENSE_RANK() OVER (PARTITION BY page ORDER BY event_time) dense_rnk,
    RANK() OVER (PARTITION BY event_type ORDER BY event_time DESC) event_time_rank
FROM web_events;
