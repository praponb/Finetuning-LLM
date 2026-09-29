SELECT
    id,
    session_id,
    event_type,
    page,
    event_time,
    duration_sec,
    SUM(duration_sec) OVER w_recent AS running_duration_sec,
    ROW_NUMBER() OVER (PARTITION BY session_id ORDER BY duration_sec DESC, id) AS event_seq,
    NTILE(5) OVER (PARTITION BY page ORDER BY duration_sec DESC, id) AS duration_sec_bucket,
    NTILE(4) OVER w_part AS duration_sec_bucket_2,
    ROW_NUMBER() OVER (PARTITION BY page ORDER BY event_time DESC, id) AS event_seq_2
FROM web_events
WHERE page IN ('/product', '/home')
WINDOW w_recent AS (ORDER BY event_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), w_part AS (PARTITION BY session_id ORDER BY duration_sec, id);
