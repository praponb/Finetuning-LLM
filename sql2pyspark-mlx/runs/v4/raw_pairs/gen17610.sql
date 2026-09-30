WITH metrics AS (
    -- Ranking and gap features
    SELECT
        id,
        store_name,
        city,
        region,
        opened_date,
        sqft,
        AVG(sqft) OVER (PARTITION BY region ORDER BY sqft, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS sqft_avg_2_prior,
        FIRST_VALUE(sqft) IGNORE NULLS OVER (PARTITION BY region ORDER BY sqft, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_sqft,
        NTILE(5) OVER (PARTITION BY city ORDER BY sqft, id) AS sqft_bucket
    FROM stores
),
cleaned AS (
    -- Keep recent, valid rows
    SELECT *, CEIL(first_known_sqft) AS first_known_sqft_whole
    FROM metrics
    WHERE opened_date > DATE '2024-01-15'
      AND region IS NOT NULL
),
not_flagged AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM cleaned s
    WHERE EXISTS (
            SELECT 1
            FROM store_sales x
            WHERE x.store_id = s.id
              AND x.sale_date >= '2025-08-25'
        )
)
-- Multi-level aggregation
SELECT
    CASE WHEN sqft >= 14748 THEN 'LARGE' ELSE 'SMALL' END AS sqft_tier,
    DATE_TRUNC('QUARTER', opened_date) AS quarter_start,
    ROUND(SUM(first_known_sqft_whole) / NULLIF(COUNT(CASE WHEN region = 'West' THEN 1 END), 0), 2) AS first_known_sqft_whole_per_west,
    COUNT(*) AS row_count,
    COUNT_IF(city = 'New York') AS new_york_events
FROM not_flagged
GROUP BY CUBE (CASE WHEN sqft >= 14748 THEN 'LARGE' ELSE 'SMALL' END, DATE_TRUNC('QUARTER', opened_date))
ORDER BY new_york_events DESC NULLS LAST;
