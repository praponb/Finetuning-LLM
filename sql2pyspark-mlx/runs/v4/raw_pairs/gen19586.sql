WITH remaining_keys AS (
    -- Set difference on keys
    SELECT id, page
    FROM web_events
    EXCEPT
    SELECT id, page
    FROM web_events
    WHERE event_type IN ('page_view', 'purchase', 'search')
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM web_events s
    INNER JOIN remaining_keys k ON s.id = k.id AND s.page = k.page
)
-- Multi-level aggregation
SELECT
    event_type,
    CASE WHEN duration_sec >= 426 THEN 'LARGE' ELSE 'SMALL' END AS duration_sec_tier,
    MAX(event_time) AS last_event_time,
    COUNT(CASE WHEN page = '/product' THEN 1 END) AS product_count,
    GROUPING_ID() AS grouping_id
FROM reconciled
GROUP BY GROUPING SETS ((event_type, CASE WHEN duration_sec >= 426 THEN 'LARGE' ELSE 'SMALL' END), (event_type), ());
