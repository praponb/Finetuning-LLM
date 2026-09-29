WITH kept_keys AS (
    SELECT id
    FROM web_events
    EXCEPT ALL
    SELECT id
    FROM web_events
    WHERE duration_sec < 370
),
verified AS (
    SELECT s.*
    FROM web_events s
    JOIN kept_keys k ON s.id = k.id
)
SELECT
    s.id,
    s.session_id,
    s.event_type,
    s.page,
    s.event_time,
    (SELECT MIN(x.duration_sec) FROM web_events x WHERE x.session_id = s.session_id) AS min_web_event_duration_sec
FROM verified s
ORDER BY session_id DESC NULLS LAST, id
