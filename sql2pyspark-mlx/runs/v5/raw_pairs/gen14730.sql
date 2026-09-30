WITH with_gaps AS (
    SELECT *, CASE WHEN LAG(event_time) OVER (PARTITION BY page ORDER BY event_time, id) IS NULL
                 OR event_time > LAG(event_time) OVER (PARTITION BY page ORDER BY event_time, id) + INTERVAL 14 DAYS THEN 1 ELSE 0 END AS new_group_flag
    FROM web_events
),
sessions_web_events AS (
    SELECT
        id,
        event_type,
        page,
        event_time,
        duration_sec,
        SUM(new_group_flag) OVER (PARTITION BY page ORDER BY event_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS island_id
    FROM with_gaps
)
SELECT
    event_type,
    AVG(duration_sec) FILTER (WHERE event_type = 'add_to_cart') AS add_to_cart_avg_duration_sec,
    SUM(island_id) AS total_island_id
FROM sessions_web_events
GROUP BY 1;
