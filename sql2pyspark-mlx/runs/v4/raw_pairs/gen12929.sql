WITH remaining_keys AS (
    -- Set intersection on keys
    SELECT customer_id
    FROM sessions
    INTERSECT ALL
    SELECT customer_id
    FROM sessions
    WHERE TO_DATE(started_at) >= '2025-06-01'
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM sessions s
    INNER JOIN remaining_keys k ON s.customer_id = k.customer_id
),
kept AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM kept_rows s
    WHERE EXISTS (
            SELECT 1
            FROM web_events c
            WHERE c.session_id = s.id
              AND c.event_type IN ('search', 'click', 'page_view')
        )
),
enriched AS (
    -- Attach customers attributes
    SELECT
        src.*,
        b.segment,
        b.name,
        b.email
    FROM kept src
    LEFT JOIN customers b
        ON src.customer_id = b.id
),
with_dims AS (
    -- Attach customers attributes
    SELECT e.*, d.email AS customer_email
    FROM enriched e
    LEFT JOIN customers d
        ON e.customer_id = d.id
),
eligible AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM with_dims s
    WHERE NOT EXISTS (
            SELECT 1
            FROM web_events c
            WHERE c.session_id = s.id
              AND c.event_time < s.started_at
        )
)
-- Aggregate with conditional metrics
SELECT
    DATE_TRUNC('MONTH', started_at) AS activity_month,
    (UNIX_TIMESTAMP(MAX(started_at)) - UNIX_TIMESTAMP(MIN(started_at))) / 60 AS duration_min,
    COUNT(CASE WHEN segment = 'Corporate' THEN 1 END) AS corporate_count,
    MIN(started_at) AS started_at,
    MIN(started_at) AS first_seen,
    COUNT_IF(segment = 'SMB') AS smb_events,
    COUNT(*) AS cnt
FROM eligible
GROUP BY 1
