WITH session_stats AS (
    SELECT session_id, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(event_type))) AS event_type_values, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(page))) AS page_values
    FROM web_events
    WHERE event_type NOT IN ('add_to_cart', 'purchase')
    GROUP BY session_id
)
SELECT
    s.id,
    s.started_at,
    ss.event_type_values,
    ss.page_values
FROM sessions s
LEFT JOIN session_stats ss ON s.id = ss.session_id;
