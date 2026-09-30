SELECT
    s.device,
    s.started_at,
    t.duration_sec_count,
    t.total_duration_sec
FROM sessions s
JOIN (SELECT session_id, COUNT(duration_sec) AS duration_sec_count, SUM(duration_sec) AS total_duration_sec
    FROM web_events
    WHERE duration_sec BETWEEN 163 AND 475
    GROUP BY session_id) t ON s.id = t.session_id
WHERE t.total_duration_sec > 1000;
