WITH gaps AS (
    SELECT *, CASE WHEN LAG(event_time) OVER (PARTITION BY page ORDER BY event_time, id) IS NULL
                 OR event_time > LAG(event_time) OVER (PARTITION BY page ORDER BY event_time, id) + INTERVAL 14 DAYS THEN 1 ELSE 0 END AS new_group_flag
    FROM web_events
),
islands AS (
    SELECT *, SUM(new_group_flag) OVER (PARTITION BY page ORDER BY event_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS island_id
    FROM gaps
)
SELECT
    id,
    page,
    event_time,
    duration_sec,
    new_group_flag,
    island_id
FROM islands
ORDER BY event_time NULLS LAST;
