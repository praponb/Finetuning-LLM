SELECT session_id, event_type_list, duration_sec_range
FROM (
    SELECT session_id, SORT_ARRAY(COLLECT_SET(event_type)) AS event_type_list, MAX(duration_sec) - MIN(duration_sec) AS duration_sec_range
    FROM web_events
    GROUP BY session_id
) AS sub
WHERE duration_sec_range >= 69
ORDER BY session_id DESC
