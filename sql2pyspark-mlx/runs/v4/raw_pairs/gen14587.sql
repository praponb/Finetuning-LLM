WITH gaps AS (
    SELECT *, CASE WHEN LAG(event_time) OVER (PARTITION BY session_id ORDER BY event_time, id) IS NULL
                 OR event_time > LAG(event_time) OVER (PARTITION BY session_id ORDER BY event_time, id) + INTERVAL '90' DAY THEN 1 ELSE 0 END AS is_new_session
    FROM web_events
),
sessions_web_events AS (
    SELECT *, SUM(is_new_session) OVER (PARTITION BY session_id ORDER BY event_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS island_id
    FROM gaps
)
SELECT
    page,
    COUNT(DISTINCT page) AS distinct_pages,
    SORT_ARRAY(COLLECT_SET(event_type)) AS event_type_list
FROM sessions_web_events
GROUP BY page;
