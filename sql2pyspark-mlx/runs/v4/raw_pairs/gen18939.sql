SELECT
    *,
    SUM(duration_sec) OVER (PARTITION BY event_type ORDER BY event_time ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_duration_sec,
    SUM(duration_sec) OVER w_running AS page_sum_duration_sec
FROM web_events
WINDOW w_running AS (PARTITION BY page)
