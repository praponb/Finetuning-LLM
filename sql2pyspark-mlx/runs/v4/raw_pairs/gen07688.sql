SELECT sessions.browser, sessions.customer_id, t.event_type_list
FROM sessions
INNER JOIN (SELECT session_id, SORT_ARRAY(COLLECT_SET(event_type)) AS event_type_list
    FROM web_events
    WHERE event_type <> 'purchase'
    GROUP BY session_id) t ON sessions.id = t.session_id
