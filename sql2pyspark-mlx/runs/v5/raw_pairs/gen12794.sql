WITH base AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        SUBSTRING(region, 1, 4) AS region_prefix,
        COALESCE(region, 'N/A') AS region_clean
    FROM stores
    WHERE opened_date >= DATE '2023-05-25'
      AND region IS NOT NULL
),
with_latest AS (
    -- Latest matching store_sale per row (LATERAL ... LIMIT 1)
    SELECT s.*, latest.store_sale_sale_date
    FROM base s
    LEFT JOIN LATERAL (
            SELECT c.sale_date AS store_sale_sale_date
            FROM store_sales c
            WHERE c.store_id = s.id
            ORDER BY c.units DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        LAG(sqft) OVER (PARTITION BY city, region ORDER BY sqft ASC, id ASC) AS prev_sqft,
        LEAD(sqft) OVER (PARTITION BY city, region ORDER BY sqft ASC, id ASC) AS next_sqft,
        ROW_NUMBER() OVER (PARTITION BY city, region ORDER BY sqft ASC, id ASC) AS row_seq
    FROM with_latest
    WHERE sqft < 12729
),
latest AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM scored
    QUALIFY ROW_NUMBER() OVER (PARTITION BY city ORDER BY sqft ASC, id ASC) = 1
),
kept AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM latest s
    WHERE NOT EXISTS (
            SELECT 1
            FROM store_sales ch
            WHERE ch.store_id = s.id
              AND ch.sale_date <= s.store_sale_sale_date
        )
)
-- Aggregate with conditional metrics
SELECT
    region,
    APPROX_PERCENTILE(prev_sqft, 0.9) AS prev_sqft_p90,
    ROUND(SUM(next_sqft) / NULLIF(COUNT(CASE WHEN region = 'Central' THEN 1 END), 0), 2) AS next_sqft_per_central
FROM kept
GROUP BY region
ORDER BY region ASC;
