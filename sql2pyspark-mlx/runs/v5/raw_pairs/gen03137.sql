SELECT
    event_type,
    session_id,
    page_values,
    min_duration_sec
FROM (
    SELECT
        event_type,
        session_id,
        CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(page))) AS page_values,
        MIN(duration_sec) AS min_duration_sec
    FROM web_events
    GROUP BY event_type, session_id
) AS t
WHERE min_duration_sec < 450
ORDER BY page_values DESC;
