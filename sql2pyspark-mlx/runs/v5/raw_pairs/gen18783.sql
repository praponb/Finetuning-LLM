SELECT
    sessions.id,
    sessions.started_at,
    t.max_duration_sec,
    t.stddev_duration_sec
FROM sessions
LEFT JOIN (SELECT session_id, MAX(duration_sec) AS max_duration_sec, ROUND(STDDEV(duration_sec), 2) AS stddev_duration_sec
    FROM web_events
    WHERE duration_sec IS NOT NULL
    GROUP BY session_id) t ON sessions.id = t.session_id;
