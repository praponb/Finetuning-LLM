WITH cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        store_name,
        opened_date,
        sqft,
        ROUND(sqft * 1.1, 2) AS adjusted_sqft,
        TRIM(region) AS clean_region,
        COALESCE(region, 'UNKNOWN') AS resolved_region
    FROM stores
    WHERE opened_date > TO_DATE('2024-01-21')
),
not_flagged AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM cleaned s
    WHERE NOT EXISTS (
            SELECT 1
            FROM orders ch
            WHERE ch.store_id = s.id
              AND ch.shipping_fee > 32
        )
),
metrics AS (
    -- Ranking and gap features
    SELECT
        id,
        store_name,
        opened_date,
        sqft,
        adjusted_sqft,
        clean_region,
        resolved_region,
        PERCENT_RANK() OVER (ORDER BY sqft NULLS FIRST) AS sqft_percent_rank,
        SUM(adjusted_sqft) OVER (ORDER BY opened_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_adjusted_sqft
    FROM not_flagged
),
kept_keys AS (
    -- Set difference on keys
    SELECT id
    FROM metrics
    EXCEPT
    SELECT id
    FROM stores
    WHERE sqft != 6377
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM metrics s
    JOIN kept_keys k ON s.id = k.id
),
not_flagged_stores AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM kept_rows s
    WHERE EXISTS (
            SELECT 1
            FROM store_sales c
            WHERE c.store_id = s.id
              AND c.sale_date < s.opened_date
        )
),
summary AS (
    -- Grouped metrics
    SELECT
        DATE_TRUNC('WEEK', opened_date) AS week_start,
        PERCENTILE_APPROX(adjusted_sqft, ARRAY(0.25, 0.5, 0.75)) AS adjusted_sqft_quartiles,
        ROUND(AVG(sqft), 2) AS avg_sqft,
        APPROX_PERCENTILE(sqft_percent_rank, ARRAY(0.25, 0.5, 0.75)) AS sqft_percent_rank_quartiles
    FROM not_flagged_stores
    GROUP BY DATE_TRUNC('WEEK', opened_date)
    HAVING ROUND(AVG(sqft), 2) < 0
)
SELECT
    week_start,
    adjusted_sqft_quartiles,
    avg_sqft
FROM summary
ORDER BY avg_sqft DESC NULLS LAST
