WITH cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        DATE_TRUNC('WEEK', event_time) AS event_time_period,
        INITCAP(TRIM(event_type)) AS event_type_title,
        COALESCE(page, 'N/A') AS page_clean
    FROM web_events
    WHERE page IS NOT NULL
      AND duration_sec IS NOT NULL
)
-- Native SQL PIVOT
SELECT *
FROM (
    SELECT
        session_id,
        page,
        event_type,
        duration_sec
    FROM cleaned
) src
PIVOT (
    SUM(duration_sec)
    FOR event_type IN ('click', 'page_view')
)
