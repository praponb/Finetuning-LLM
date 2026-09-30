SELECT sessions.device, sessions.started_at, t.page_values
FROM sessions
LEFT JOIN (SELECT session_id, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(page))) AS page_values
    FROM web_events
    GROUP BY session_id) t ON sessions.id = t.session_id
