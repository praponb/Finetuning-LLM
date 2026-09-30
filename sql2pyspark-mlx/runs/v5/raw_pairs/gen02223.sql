SELECT
    s.id,
    s.customer_id,
    s.browser,
    t.max_duration_sec,
    t.stddev_duration_sec
FROM sessions s
LEFT JOIN (SELECT session_id, MAX(duration_sec) AS max_duration_sec, ROUND(STDDEV(duration_sec), 2) AS stddev_duration_sec
    FROM web_events
    GROUP BY session_id) t ON s.id = t.session_id
WHERE t.max_duration_sec > 420
