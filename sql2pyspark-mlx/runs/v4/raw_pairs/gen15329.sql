SELECT
    s.id,
    s.browser,
    s.country,
    t.avg_duration_sec,
    t.checkout_count
FROM sessions s
LEFT JOIN (SELECT session_id, ROUND(AVG(duration_sec), 2) AS avg_duration_sec, SUM(CASE WHEN page = '/checkout' THEN 1 ELSE 0 END) AS checkout_count
    FROM web_events
    WHERE duration_sec BETWEEN 123 AND 445
    GROUP BY session_id) t ON s.id = t.session_id
ORDER BY avg_duration_sec DESC;
