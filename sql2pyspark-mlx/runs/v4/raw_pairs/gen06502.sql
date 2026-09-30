SELECT
    *,
    SUM(duration_sec) OVER (PARTITION BY page) AS page_sum_duration_sec,
    SUM(duration_sec) OVER (PARTITION BY session_id, event_type ORDER BY duration_sec, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_duration_sec
FROM web_events;
