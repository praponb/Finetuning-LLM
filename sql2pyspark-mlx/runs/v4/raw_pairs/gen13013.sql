SELECT
    id,
    session_id,
    duration_sec,
    event_time,
    PERCENT_RANK() OVER (PARTITION BY session_id ORDER BY duration_sec DESC) AS duration_sec_pct_rank,
    ROUND(AVG(duration_sec) OVER (PARTITION BY session_id ORDER BY event_time, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2) AS duration_sec_moving_avg_7
FROM web_events
ORDER BY duration_sec_moving_avg_7 DESC, duration_sec_pct_rank
