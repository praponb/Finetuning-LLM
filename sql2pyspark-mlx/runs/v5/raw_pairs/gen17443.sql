WITH latest AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM web_events
    QUALIFY ROW_NUMBER() OVER (PARTITION BY session_id ORDER BY event_time DESC, id) = 1
)
SELECT
    s.id,
    s.session_id,
    s.event_type,
    s.event_time,
    (SELECT SUM(x.duration_sec) FROM web_events x WHERE x.session_id = s.session_id) AS sum_web_event_duration_sec
FROM latest s
ORDER BY id;
