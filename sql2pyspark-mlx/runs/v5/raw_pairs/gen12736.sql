WITH enriched AS (
    -- Attach sessions attributes
    SELECT src.*, b.browser
    FROM web_events src
    INNER JOIN sessions b
        ON src.session_id = b.id
    WHERE b.device NOT IN ('desktop', 'mobile')
)
-- Multi-level aggregation
SELECT
    DATE_TRUNC('YEAR', event_time) AS year_start,
    COUNT_IF(event_type = 'search') AS search_events,
    COUNT(DISTINCT id) AS distinct_ids,
    ROUND(SUM(duration_sec) / NULLIF(COUNT(CASE WHEN event_type = 'add_to_cart' THEN 1 END), 0), 2) AS duration_sec_per_add_to_cart
FROM enriched
GROUP BY DATE_TRUNC('YEAR', event_time)
ORDER BY search_events DESC;
