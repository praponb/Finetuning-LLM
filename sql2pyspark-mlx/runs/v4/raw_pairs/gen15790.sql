WITH kept_keys AS (
    -- Set difference on keys
    SELECT device, customer_id
    FROM sessions
    EXCEPT
    SELECT device, customer_id
    FROM sessions
    WHERE device IN ('tablet', 'mobile', 'desktop')
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM sessions s
    JOIN kept_keys k ON s.device = k.device AND s.customer_id = k.customer_id
)
-- Multi-level aggregation
SELECT
    country,
    COUNT(*) AS event_count,
    (UNIX_TIMESTAMP(MAX(started_at)) - UNIX_TIMESTAMP(MIN(started_at))) / 60 AS span_minutes,
    SORT_ARRAY(COLLECT_SET(device)) AS device_list,
    (UNIX_TIMESTAMP(MAX(started_at)) - UNIX_TIMESTAMP(MIN(started_at))) / 60 AS span_minutes_2,
    MIN(started_at) AS first_started_at
FROM kept_rows
GROUP BY 1;
