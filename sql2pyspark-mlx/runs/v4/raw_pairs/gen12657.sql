SELECT
    s.browser,
    YEAR(s.started_at) AS started_year,
    SORT_ARRAY(COLLECT_SET(we.page)) AS page_list,
    MAX(we.event_time) AS last_event_time
FROM web_events we
LEFT JOIN sessions s ON s.id = we.session_id
WHERE TO_DATE(we.event_time) >= '2024-07-01'
GROUP BY s.browser, YEAR(s.started_at)
