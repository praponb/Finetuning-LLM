WITH src AS (
    SELECT *
    FROM web_events
    WHERE duration_sec <= 435
),
Summary AS (
    SELECT
        page,
        event_type,
        SORT_ARRAY(COLLECT_SET(page)) page_list,
        MIN(event_time) first_event_time
    FROM src
    GROUP BY page, event_type
)
SELECT
    page,
    event_type,
    page_list,
    first_event_time
FROM Summary
ORDER BY first_event_time;
