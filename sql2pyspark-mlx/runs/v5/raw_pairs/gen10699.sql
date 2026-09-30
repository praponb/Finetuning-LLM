SELECT
    s.started_at,
    s.browser,
    t.event_type_values,
    t.avg_duration_sec
FROM sessions s
JOIN (SELECT session_id, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(event_type))) AS event_type_values, ROUND(AVG(duration_sec), 2) AS avg_duration_sec
    FROM web_events
    GROUP BY session_id) t ON s.id = t.session_id
WHERE t.avg_duration_sec > 73;
