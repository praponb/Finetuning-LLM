WITH matched_keys AS (
    -- Set difference on keys
    SELECT device, id
    FROM sessions
    EXCEPT DISTINCT
    SELECT device, id
    FROM sessions
    WHERE started_at < '2025-03-01'
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM sessions s
    INNER JOIN matched_keys k ON s.device = k.device AND s.id = k.id
),
base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        customer_id,
        device,
        browser,
        started_at,
        CAST(started_at AS DATE) AS started_at_day
    FROM kept_rows
    WHERE started_at >= TIMESTAMP '2023-10-03 07:00:00'
),
valid AS (
    -- Keep recent, valid rows
    SELECT
        id,
        customer_id,
        device,
        browser,
        started_at,
        started_at_day,
        DATE_TRUNC('WEEK', started_at) AS started_at_period,
        ADD_MONTHS(started_at_day, 6) AS started_at_day_plus_months
    FROM base
    WHERE browser IS NOT NULL
      AND customer_id = 12
)
-- Grouped metrics
SELECT
    customer_id,
    COUNT(CASE WHEN device = 'tablet' THEN 1 END) AS tablet_count,
    COUNT(DISTINCT customer_id) AS distinct_customer_ids
FROM valid
GROUP BY customer_id
ORDER BY tablet_count ASC NULLS LAST
