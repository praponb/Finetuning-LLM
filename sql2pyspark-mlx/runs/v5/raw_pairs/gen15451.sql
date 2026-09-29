WITH metrics AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        COUNT(sqft) OVER (PARTITION BY region, city) region_count_sqft,
        ROUND(sqft / SUM(sqft) OVER (PARTITION BY city), 4) sqft_share,
        FIRST_VALUE(sqft) IGNORE NULLS OVER (PARTITION BY city ORDER BY opened_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) first_known_sqft,
        ROW_NUMBER() OVER (PARTITION BY region, city ORDER BY sqft DESC, id) event_seq
    FROM stores
),
metrics_stores AS (
    -- Running and moving aggregates
    SELECT
        id,
        region,
        opened_date,
        region_count_sqft,
        first_known_sqft,
        ROUND(region_count_sqft / SUM(region_count_sqft) OVER (PARTITION BY region, city), 4) region_count_sqft_share,
        LAG(sqft_share, 2) OVER (ORDER BY first_known_sqft, id) prev_sqft_share,
        SUM(sqft_share) OVER (PARTITION BY region ORDER BY event_seq, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_sqft_share
    FROM metrics
),
scored AS (
    -- Ranking and gap features
    SELECT
        *,
        SUM(first_known_sqft) OVER (PARTITION BY region ORDER BY running_sqft_share, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_first_known_sqft,
        LEAD(running_sqft_share, 2, 0) OVER (PARTITION BY region ORDER BY region_count_sqft_share, id) next_running_sqft_share,
        NTILE(10) OVER (PARTITION BY region ORDER BY prev_sqft_share DESC, id) prev_sqft_share_bucket,
        CUME_DIST() OVER (PARTITION BY region ORDER BY first_known_sqft DESC) first_known_sqft_cume_dist
    FROM metrics_stores
),
latest AS (
    -- QUALIFY-style window filter
    SELECT *, RANK() OVER (PARTITION BY region ORDER BY first_known_sqft) AS rn
    FROM scored
    QUALIFY rn = 1
),
latest_match AS (
    -- Latest matching store_sale per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.opened_date,
        s.region_count_sqft,
        s.region_count_sqft_share,
        s.running_sqft_share,
        s.first_known_sqft_cume_dist,
        x.units
    FROM latest s
    JOIN LATERAL (
            SELECT c.units
            FROM store_sales c
            WHERE c.store_id = s.id
            ORDER BY c.sale_date DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
kept AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM latest_match s
    WHERE NOT EXISTS (
            SELECT 1
            FROM orders x
            WHERE x.store_id = s.id
              AND x.channel IN ('partner', 'store')
        )
),
kept_stores AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM kept s
    WHERE NOT EXISTS (
            SELECT 1
            FROM store_sales ch
            WHERE ch.store_id = s.id
              AND ch.revenue <= 3800
        )
)
-- Grouped metrics
SELECT
    CASE WHEN units >= 33 THEN 'LARGE' ELSE 'SMALL' END AS units_tier,
    COUNT(DISTINCT id) AS distinct_ids,
    ROUND(AVG(running_sqft_share), 2) AS avg_running_sqft_share,
    COUNT(DISTINCT id) AS distinct_ids_2
FROM kept_stores
GROUP BY units_tier
HAVING distinct_ids < 1000;
