SELECT s.customer_id, t.page_view_duration_sec
FROM sessions s
INNER JOIN (SELECT session_id, SUM(CASE WHEN event_type = 'page_view' THEN duration_sec ELSE 0 END) AS page_view_duration_sec
    FROM web_events
    GROUP BY session_id) t ON s.id = t.session_id
WHERE t.page_view_duration_sec > 79
ORDER BY customer_id DESC
