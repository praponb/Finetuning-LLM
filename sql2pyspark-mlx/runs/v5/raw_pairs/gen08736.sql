SELECT s.id, s.customer_id, t.page_view_count
FROM sessions s
JOIN (SELECT session_id, SUM(CASE WHEN event_type = 'page_view' THEN 1 ELSE 0 END) AS page_view_count
    FROM web_events
    GROUP BY session_id) t ON s.id = t.session_id
