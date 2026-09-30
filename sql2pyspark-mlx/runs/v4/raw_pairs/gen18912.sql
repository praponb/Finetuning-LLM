SELECT
    *,
    COUNT(duration_sec) OVER (PARTITION BY event_type, page) AS event_type_count_duration_sec,
    AVG(duration_sec) OVER (PARTITION BY session_id ORDER BY event_time, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS duration_sec_avg_6_prior,
    SUM(duration_sec) OVER (PARTITION BY page ORDER BY duration_sec, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_duration_sec
FROM web_events;
