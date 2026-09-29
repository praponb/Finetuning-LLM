WITH cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        LOWER(page) page_lower,
        COALESCE(page, 'UNKNOWN') resolved_page
    FROM web_events
    WHERE event_time > TO_DATE('2023-06-05')
)
-- Native SQL PIVOT
SELECT *
FROM (
    SELECT
        page,
        event_type,
        duration_sec
    FROM cleaned
) src
PIVOT (
    AVG(duration_sec) AS value_sum,
    COUNT(1) AS item_count
    FOR event_type IN ('add_to_cart', 'search', 'page_view')
)
