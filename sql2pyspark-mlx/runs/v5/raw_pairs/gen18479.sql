SELECT
    *,
    LEAD(duration_sec) OVER w_running AS next_duration_sec,
    ROW_NUMBER() OVER w_running AS row_seq,
    ROW_NUMBER() OVER (ORDER BY duration_sec DESC, id) AS seq_no
FROM web_events
WINDOW w_running AS (PARTITION BY page, session_id ORDER BY event_time, id)
