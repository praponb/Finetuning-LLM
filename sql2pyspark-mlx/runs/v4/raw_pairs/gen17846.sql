WITH filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        TO_DATE(event_time) AS event_time_date,
        ROUND(duration_sec, 0) AS duration_sec_rounded
    FROM web_events
    WHERE event_time > TIMESTAMP '2023-04-03 23:00:00'
      AND event_type != 'page_view'
)
-- Multi-level aggregation
SELECT
    CASE WHEN duration_sec >= 525 THEN 'PREMIUM' ELSE 'STANDARD' END AS duration_sec_tier,
    COUNT_IF(event_type = 'add_to_cart') AS add_to_cart_events,
    COUNT(DISTINCT id) AS distinct_ids,
    COUNT(*) FILTER (WHERE event_type = 'purchase') AS purchase_rows,
    SORT_ARRAY(COLLECT_SET(page)) AS page_list,
    MIN(event_time) AS started_at
FROM filtered
GROUP BY duration_sec_tier
HAVING COUNT(*) FILTER (WHERE event_type = 'purchase') >= 1;
