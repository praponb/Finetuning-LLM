SELECT
    s.started_at,
    s.device,
    t.stddev_duration_sec,
    t.avg_duration_sec
FROM sessions s
INNER JOIN (SELECT session_id, ROUND(STDDEV(duration_sec), 2) AS stddev_duration_sec, ROUND(AVG(duration_sec), 2) AS avg_duration_sec
    FROM web_events
    WHERE page = '/checkout'
    GROUP BY session_id) t ON s.id = t.session_id
ORDER BY t.avg_duration_sec
