WITH cleaned AS (
    SELECT
        *,
        INITCAP(TRIM(event_type)) event_type_title,
        COALESCE(page, 'N/A') page_clean
    FROM web_events
    WHERE event_time > DATE '2024-04-29'
)
SELECT page, id
FROM cleaned
EXCEPT DISTINCT
SELECT page, id
FROM web_events
WHERE duration_sec = 248;
