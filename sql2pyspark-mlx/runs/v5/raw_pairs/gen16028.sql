WITH remaining_keys AS (
    -- Set difference on keys
    SELECT id, event_type
    FROM web_events
    EXCEPT
    SELECT id, event_type
    FROM web_events
    WHERE event_type IN ('page_view', 'purchase', 'add_to_cart')
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM web_events s
    INNER JOIN remaining_keys k ON s.id = k.id AND s.event_type = k.event_type
)
-- Grouped metrics
SELECT
    event_type,
    BIT_AND(duration_sec) AS duration_sec_bit_and,
    COUNT(CASE WHEN page = '/cart' THEN 1 END) AS cart_count,
    AVG(duration_sec) FILTER (WHERE event_type = 'page_view') AS page_view_avg_duration_sec,
    MIN(event_time) AS first_seen
FROM kept_rows
GROUP BY 1
ORDER BY cart_count DESC;
