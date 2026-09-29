WITH gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN event_time - LAG(event_time) OVER (PARTITION BY session_id ORDER BY event_time, id) > INTERVAL 7 DAYS THEN 1 ELSE 0 END AS is_new_burst
    FROM web_events
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_burst) OVER (PARTITION BY session_id ORDER BY event_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM gaps
)
SELECT
    id,
    session_id,
    event_type,
    event_time,
    duration_sec,
    is_new_burst,
    session_no,
    CASE WHEN event_type RLIKE '^(search|purchase)$' THEN 'GROUP_A' ELSE 'CORE' END AS event_type_zone
FROM sessionized
ORDER BY event_type DESC, event_time;
