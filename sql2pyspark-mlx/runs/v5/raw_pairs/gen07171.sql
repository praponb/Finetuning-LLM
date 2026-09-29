WITH filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        store_name,
        city,
        region,
        opened_date,
        sqft,
        CEIL(sqft) sqft_whole,
        INITCAP(TRIM(city)) city_title,
        COALESCE(region, 'DIRECT') resolved_region
    FROM stores
    WHERE opened_date >= DATE '2023-04-16'
      AND region IS NOT NULL
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        store_name,
        city,
        region,
        sqft_whole,
        ROUND(sqft / SUM(sqft) OVER (PARTITION BY region), 4) sqft_share,
        CUME_DIST() OVER (ORDER BY sqft NULLS FIRST) sqft_cume_dist,
        AVG(sqft) OVER (PARTITION BY city ORDER BY sqft, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) sqft_avg_3_prior,
        AVG(sqft_whole) OVER (PARTITION BY city ORDER BY opened_date, id ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) sqft_whole_avg_5,
        ROW_NUMBER() OVER (PARTITION BY city, region ORDER BY opened_date DESC, id) row_seq
    FROM filtered
),
not_flagged AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM metrics s
    WHERE NOT EXISTS (
            SELECT 1
            FROM store_sales x
            WHERE x.store_id = s.id
              AND x.revenue IS NOT NULL
        )
),
not_flagged_stores AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM not_flagged s
    WHERE EXISTS (
            SELECT 1
            FROM store_sales ch
            WHERE ch.store_id = s.id
              AND ch.sale_date <= '2025-01-01'
        )
),
with_latest AS (
    -- Latest matching order per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.region,
        s.sqft_whole,
        s.sqft_share,
        s.sqft_cume_dist,
        s.sqft_avg_3_prior,
        s.sqft_whole_avg_5,
        s.row_seq,
        latest.amount
    FROM not_flagged_stores s
    LEFT JOIN LATERAL (
            SELECT c.amount
            FROM orders c
            WHERE c.store_id = s.id
            ORDER BY c.order_date DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
)
-- Aggregate with conditional metrics
SELECT
    CASE WHEN row_seq >= 8 THEN 'PREMIUM' ELSE 'STANDARD' END AS row_seq_tier,
    region,
    COUNT(*) AS num_rows,
    SUM(sqft_whole_avg_5) FILTER (WHERE region = 'South') AS south_total_sqft_whole_avg_5,
    SIZE(COLLECT_SET(region)) AS num_regions,
    PERCENT_RANK() OVER (PARTITION BY CASE WHEN row_seq >= 8 THEN 'PREMIUM' ELSE 'STANDARD' END ORDER BY SUM(sqft_whole_avg_5) FILTER (WHERE region = 'South') DESC) AS south_total_sqft_whole_avg_5_percent_rank
FROM with_latest
GROUP BY row_seq_tier, region
HAVING SUM(sqft_whole_avg_5) FILTER (WHERE region = 'South') > 0
