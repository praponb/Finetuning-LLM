WITH deduped AS (
    -- Keep one row per key (latest first)
    SELECT
        id,
        city,
        region,
        opened_date,
        sqft
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY region, city ORDER BY opened_date DESC NULLS FIRST, id) AS rank_in_group
        FROM stores
    ) ranked
    WHERE rank_in_group = 1
),
matched_keys AS (
    -- Set difference on keys
    SELECT city
    FROM deduped
    EXCEPT DISTINCT
    SELECT city
    FROM stores
    WHERE region = 'North'
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM deduped s
    INNER JOIN matched_keys k ON s.city = k.city
),
enriched AS (
    -- Ranking and gap features
    SELECT
        id,
        city,
        region,
        opened_date,
        sqft,
        ROUND(sqft / SUM(sqft) OVER (PARTITION BY city), 4) AS sqft_share,
        COUNT(sqft) OVER (PARTITION BY region) AS region_count_sqft,
        ROUND(sqft / SUM(sqft) OVER (PARTITION BY region), 4) AS sqft_share_2
    FROM verified
    WHERE sqft != 10590
),
eligible AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM enriched s
    WHERE NOT EXISTS (
            SELECT 1
            FROM orders x
            WHERE x.store_id = s.id
              AND YEAR(x.order_date) = 2023
        )
),
scored AS (
    -- Running and moving aggregates
    SELECT *, ROUND(sqft / SUM(sqft) OVER (PARTITION BY region, city), 4) AS sqft_share_3
    FROM eligible
),
base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        city,
        region,
        sqft_share,
        region_count_sqft,
        sqft_share_2,
        ROUND(sqft_share, 0) AS sqft_share_rounded,
        COALESCE(region, 'N/A') AS resolved_region
    FROM scored
    WHERE opened_date >= DATE '2023-05-03'
      AND region IN ('North', 'Central')
),
kept AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM base s
    WHERE NOT EXISTS (
            SELECT 1
            FROM store_sales ch
            WHERE ch.store_id = s.id
              AND ch.sale_date < '2025-03-01'
        )
)
-- Aggregate with conditional metrics
SELECT
    city,
    CASE WHEN sqft_share_2 >= 0.89 THEN 'LARGE' ELSE 'SMALL' END AS sqft_share_2_tier,
    SORT_ARRAY(COLLECT_SET(region)) AS region_list,
    PERCENTILE_APPROX(sqft_share_rounded, 0.5) AS sqft_share_rounded_p50,
    COUNT(*) FILTER (WHERE region = 'West') AS west_rows,
    ROUND(SUM(sqft_share_rounded) / NULLIF(COUNT(CASE WHEN region = 'South' THEN 1 END), 0), 2) AS sqft_share_rounded_per_south,
    DENSE_RANK() OVER (PARTITION BY city ORDER BY PERCENTILE_APPROX(sqft_share_rounded, 0.5) DESC) AS sqft_share_rounded_p50_dense_rank
FROM kept
GROUP BY city, CASE WHEN sqft_share_2 >= 0.89 THEN 'LARGE' ELSE 'SMALL' END
ORDER BY west_rows NULLS FIRST;
