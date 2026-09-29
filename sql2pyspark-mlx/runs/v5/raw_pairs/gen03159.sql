WITH filtered AS (
    -- Keep recent, valid rows
    SELECT
        id,
        store_name,
        city,
        region,
        opened_date,
        sqft,
        SUBSTRING(city, 1, 2) AS city_prefix,
        SUBSTRING(city, 1, 2) AS city_prefix_2,
        COALESCE(region, 'DIRECT') AS region_clean
    FROM stores
    WHERE region IS NOT NULL
),
not_flagged AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM filtered s
    WHERE NOT EXISTS (
            SELECT 1
            FROM store_sales ch
            WHERE ch.store_id = s.id
              AND ch.sale_date >= s.opened_date
        )
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        SUM(sqft) OVER (PARTITION BY city ORDER BY sqft, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_sqft,
        ROUND(sqft / SUM(sqft) OVER (PARTITION BY region), 4) AS sqft_share
    FROM not_flagged
),
latest AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY city ORDER BY cumulative_sqft NULLS LAST, id) AS rn
        FROM with_windows
    ) x
    WHERE rn = 1
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        store_name,
        city,
        region,
        opened_date,
        city_prefix_2,
        sqft_share,
        rn,
        ROUND(sqft / SUM(sqft) OVER (PARTITION BY city), 4) AS sqft_share_2,
        SUM(cumulative_sqft) OVER (PARTITION BY region, city ORDER BY sqft_share, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_cumulative_sqft
    FROM latest
    WHERE sqft < 6037
),
scored_stores AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        store_name,
        city,
        sqft_share,
        rn,
        sqft_share_2,
        running_cumulative_sqft,
        SUM(running_cumulative_sqft) OVER (PARTITION BY city, region ORDER BY sqft_share_2, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_running_cumulative_sqft,
        AVG(running_cumulative_sqft) OVER (PARTITION BY city, region ORDER BY running_cumulative_sqft, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS running_cumulative_sqft_avg_2
    FROM scored
),
with_last AS (
    -- Latest matching order per row (LATERAL ... LIMIT 1)
    SELECT s.*, x.shipping_fee
    FROM scored_stores s
    LEFT JOIN LATERAL (
            SELECT c.shipping_fee
            FROM orders c
            WHERE c.store_id = s.id
            ORDER BY c.shipping_fee DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
)
SELECT
    id,
    store_name,
    city,
    sqft_share,
    rn,
    sqft_share_2,
    cumulative_running_cumulative_sqft,
    running_cumulative_sqft_avg_2,
    shipping_fee,
    CASE WHEN city RLIKE '^(London|New York)$' THEN 'PRIORITY' WHEN city RLIKE '^(Bangkok|Chiang Mai)$' THEN 'GROUP_A' ELSE 'GROUP_B' END AS city_zone
FROM with_last
ORDER BY running_cumulative_sqft_avg_2 DESC
