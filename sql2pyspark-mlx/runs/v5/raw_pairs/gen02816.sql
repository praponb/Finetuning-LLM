WITH web_events_by_session AS (
    SELECT session_id, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(page))) AS page_values
    FROM web_events
    GROUP BY session_id
)
SELECT s.id, s.country, webs.page_values
FROM sessions s
INNER JOIN web_events_by_session webs ON s.id = webs.session_id
ORDER BY webs.page_values DESC, id
LIMIT 10
