WITH filtered AS (
    -- Keep recent, valid rows
    SELECT
        *,
        DATE_TRUNC('month', opened_date) AS opened_date_period,
        COALESCE(region, 'DIRECT') AS region_clean
    FROM stores
    WHERE opened_date > DATE '2023-09-18'
),
kept_keys AS (
    -- Set intersection on keys
    SELECT id
    FROM filtered
    INTERSECT ALL
    SELECT id
    FROM stores
    WHERE store_name IS NOT NULL
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM filtered s
    JOIN kept_keys k ON s.id = k.id
),
not_flagged AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM verified s
    WHERE NOT EXISTS (
            SELECT 1
            FROM orders x
            WHERE x.store_id = s.id
              AND x.order_date < s.opened_date_period
        )
),
eligible AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM not_flagged s
    WHERE NOT EXISTS (
            SELECT 1
            FROM store_sales c
            WHERE c.store_id = s.id
              AND c.units >= 20
        )
)
-- Aggregate with conditional metrics
SELECT
    DATE_TRUNC('WEEK', opened_date_period) AS week_start,
    STDDEV_SAMP(sqft) AS sqft_stddev,
    ROUND(SUM(sqft) / NULLIF(COUNT(CASE WHEN region = 'North' THEN 1 END), 0), 2) AS sqft_per_north,
    COUNT(CASE WHEN city = 'London' THEN 1 END) AS london_count
FROM eligible
GROUP BY CUBE (DATE_TRUNC('WEEK', opened_date_period));
