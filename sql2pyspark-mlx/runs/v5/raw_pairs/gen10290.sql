SELECT
    id,
    page,
    event_time,
    duration_sec,
    PERCENT_RANK() OVER (PARTITION BY page ORDER BY event_time) AS event_time_pct_rank,
    RANK() OVER (PARTITION BY page ORDER BY duration_sec DESC) AS rnk
FROM web_events;
