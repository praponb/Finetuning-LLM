SELECT
    id,
    duration_sec,
    session_id,
    page,
    INITCAP(TRIM(page)) AS page_title
FROM web_events
WHERE page IS NULL AND event_time >= '2023-03-01'
ORDER BY session_id DESC, id
LIMIT 5;
