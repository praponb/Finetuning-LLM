SELECT
    id,
    session_id,
    duration_sec,
    event_time,
    PERCENT_RANK() OVER (PARTITION BY session_id ORDER BY duration_sec DESC) AS duration_sec_pct_rank,
    DENSE_RANK() OVER (PARTITION BY session_id ORDER BY event_time) AS dense_rnk
FROM web_events
WHERE HOUR(event_time) BETWEEN 6 AND 10;
