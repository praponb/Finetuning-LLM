SELECT
    id,
    event_type,
    AVG(duration_sec) OVER w_part AS last7_avg_duration_sec,
    MIN(duration_sec) OVER w_part AS last7_min_duration_sec,
    MAX(duration_sec) OVER w_part AS last7_max_duration_sec,
    SUM(duration_sec) OVER w_part AS last7_sum_duration_sec
FROM web_events
WINDOW w_part AS (PARTITION BY event_type, page ORDER BY event_time, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)
