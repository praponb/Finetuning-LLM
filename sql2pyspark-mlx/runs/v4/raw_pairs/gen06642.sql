WITH ranked AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY page ORDER BY event_time DESC NULLS LAST, id) AS dedup_rank
    FROM web_events
    QUALIFY dedup_rank = 1
)
SELECT
    s.id,
    s.session_id,
    s.page,
    s.event_time,
    s.duration_sec,
    s.dedup_rank,
    (SELECT MAX(t2.duration_sec) FROM web_events t2 WHERE t2.session_id = s.session_id) AS max_web_event_duration_sec
FROM ranked s;
