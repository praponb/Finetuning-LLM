WITH recent_web_events AS (
    SELECT *, LENGTH(page) AS page_length
    FROM web_events
    WHERE duration_sec BETWEEN 151 AND 185
),
event_type_summary AS (
    SELECT event_type, SUM(page_length) AS total_page_length, SORT_ARRAY(COLLECT_SET(event_type)) AS event_type_list
    FROM recent_web_events
    GROUP BY event_type
)
SELECT event_type, total_page_length
FROM event_type_summary
WHERE total_page_length > 230
ORDER BY event_type;
