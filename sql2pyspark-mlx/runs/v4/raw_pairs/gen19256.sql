SELECT
    event_type,
    session_id,
    home_count,
    event_type_list
FROM (
    SELECT
        event_type,
        session_id,
        COUNT(CASE WHEN page = '/home' THEN 1 END) AS home_count,
        SORT_ARRAY(COLLECT_SET(event_type)) AS event_type_list
    FROM web_events
    WHERE page NOT IN ('/cart', '/checkout')
    GROUP BY event_type, session_id
) AS t
WHERE home_count > 10
ORDER BY event_type, session_id
LIMIT 20;
