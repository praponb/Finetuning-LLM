WITH filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        store_name,
        city,
        region,
        opened_date,
        ROUND(sqft * 1.15, 2) AS sqft_adjusted,
        COALESCE(region, 'DIRECT') AS region_filled
    FROM stores
    WHERE opened_date >= TO_DATE('2023-08-14')
      AND region IS NOT NULL
),
remaining_keys AS (
    -- Set difference on keys
    SELECT city, id
    FROM filtered
    EXCEPT
    SELECT city, id
    FROM stores
    WHERE region IN ('North', 'West', 'Central')
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM filtered s
    JOIN remaining_keys k ON s.city = k.city AND s.id = k.id
),
not_flagged AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM verified s
    WHERE NOT EXISTS (
            SELECT 1
            FROM store_sales ch
            WHERE ch.store_id = s.id
              AND ch.sale_date >= s.opened_date
        )
)
-- Grouped metrics
SELECT
    DATE_TRUNC('YEAR', opened_date) AS year_start,
    CASE WHEN sqft_adjusted >= 65 THEN 'LARGE' ELSE 'SMALL' END AS sqft_adjusted_tier,
    PERCENTILE_APPROX(sqft_adjusted, ARRAY(0.25, 0.5, 0.75)) AS sqft_adjusted_quartiles,
    COUNT(*) AS row_count
FROM not_flagged
GROUP BY DATE_TRUNC('YEAR', opened_date), CASE WHEN sqft_adjusted >= 65 THEN 'LARGE' ELSE 'SMALL' END;
