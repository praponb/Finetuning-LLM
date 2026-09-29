WITH filtered AS (
    SELECT
        id,
        session_id,
        page,
        event_time,
        duration_sec,
        CASE WHEN page IN ('/search', '/cart') THEN 'Tier 1' ELSE 'Other' END page_group,
        COALESCE(page, 'N/A') page_filled
    FROM web_events
    WHERE page IS NOT NULL
)
SELECT page
FROM filtered
EXCEPT DISTINCT
SELECT page
FROM web_events
WHERE event_type IN ('page_view', 'purchase');
