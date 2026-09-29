WITH cleaned AS (
    SELECT
        id,
        event_type,
        CASE WHEN duration_sec >= 378 THEN 'High' WHEN duration_sec >= 82 THEN 'Medium' ELSE 'Low' END AS duration_sec_band,
        COALESCE(page, 'DIRECT') AS page_clean
    FROM web_events
    WHERE page IS NOT NULL
),
remaining_keys AS (
    SELECT event_type
    FROM cleaned
    INTERSECT
    SELECT event_type
    FROM web_events
    WHERE event_type = 'search'
),
reconciled AS (
    SELECT s.*
    FROM cleaned s
    INNER JOIN remaining_keys k ON s.event_type = k.event_type
),
cleaned_web_events AS (
    SELECT *, INITCAP(TRIM(event_type)) AS event_type_title
    FROM reconciled
    WHERE event_type = 'add_to_cart'
),
filtered AS (
    SELECT
        *,
        CASE WHEN event_type = 'purchase' THEN 1 ELSE 0 END AS is_purchase,
        SUBSTRING(event_type, 1, 4) AS event_type_prefix
    FROM cleaned_web_events
    WHERE event_type IN ('page_view', 'add_to_cart', 'purchase')
),
cleaned_web_events_2 AS (
    SELECT *, TRIM(event_type) AS clean_event_type
    FROM filtered
    WHERE is_purchase > 40
)
SELECT
    CASE WHEN is_purchase >= 82 THEN 'LARGE' ELSE 'SMALL' END AS is_purchase_tier,
    event_type,
    SORT_ARRAY(COLLECT_SET(event_type)) AS event_type_list,
    COUNT_IF(event_type = 'search') AS search_events,
    SORT_ARRAY(COLLECT_SET(event_type)) AS event_type_list_2
FROM cleaned_web_events_2
GROUP BY GROUPING SETS ((CASE WHEN is_purchase >= 82 THEN 'LARGE' ELSE 'SMALL' END, event_type), (CASE WHEN is_purchase >= 82 THEN 'LARGE' ELSE 'SMALL' END), ())
ORDER BY event_type DESC NULLS LAST
