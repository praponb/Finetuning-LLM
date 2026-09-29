SELECT s.customer_id, s.browser, t.home_count
FROM sessions s
LEFT JOIN (SELECT session_id, SUM(CASE WHEN page = '/home' THEN 1 ELSE 0 END) home_count
    FROM web_events
    WHERE HOUR(event_time) BETWEEN 12 AND 16
    GROUP BY session_id) t ON s.id = t.session_id
ORDER BY home_count DESC;
