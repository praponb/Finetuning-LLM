SELECT sessions.browser, t.event_type_list, t.stddev_duration_sec
FROM sessions
LEFT JOIN (SELECT session_id, SORT_ARRAY(COLLECT_SET(event_type)) event_type_list, ROUND(STDDEV(duration_sec), 2) stddev_duration_sec
    FROM web_events
    WHERE page = '/checkout'
    GROUP BY session_id) t ON sessions.id = t.session_id
WHERE t.stddev_duration_sec > 76
ORDER BY t.stddev_duration_sec;
