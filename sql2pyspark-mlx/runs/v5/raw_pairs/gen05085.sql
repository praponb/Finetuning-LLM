WITH with_dims AS (
    SELECT
        e.id,
        e.session_id,
        e.event_type,
        e.event_time,
        e.duration_sec,
        p.country
    FROM web_events e
    LEFT JOIN sessions p
        ON e.session_id = p.id
    WHERE HOUR(p.started_at) BETWEEN 10 AND 14
)
SELECT
    s.id,
    s.session_id,
    s.event_type,
    s.event_time,
    s.duration_sec,
    s.country,
    (SELECT MIN(x.duration_sec) FROM web_events x WHERE x.session_id = s.session_id) AS min_web_event_duration_sec
FROM with_dims s
ORDER BY country DESC;
