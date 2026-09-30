WITH valid AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        CASE WHEN city IN ('Tokyo', 'New York') THEN 'Group A' ELSE 'Other' END city_group,
        COALESCE(region, 'OTHER') region_filled
    FROM stores
    WHERE region IS DISTINCT FROM 'Central'
      AND opened_date > DATE '2023-07-22'
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        id,
        city,
        region,
        sqft,
        region_filled,
        SUM(sqft) OVER w_hist running_sum_sqft,
        MAX(sqft) OVER w_hist running_max_sqft,
        COUNT(sqft) OVER w_hist running_count_sqft,
        MIN(sqft) OVER w_hist running_min_sqft
    FROM valid
    WHERE region IN ('West', 'Central')
    WINDOW w_hist AS (PARTITION BY region, city ORDER BY sqft ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
eligible AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM with_windows s
    WHERE NOT EXISTS (
            SELECT 1
            FROM store_sales c
            WHERE c.store_id = s.id
              AND c.units < 20
        )
),
eligible_stores AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM eligible s
    WHERE EXISTS (
            SELECT 1
            FROM store_sales c
            WHERE c.store_id = s.id
              AND c.sale_date BETWEEN '2023-03-16' AND '2023-12-01'
        )
),
eligible_stores_2 AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM eligible_stores s
    WHERE NOT EXISTS (
            SELECT 1
            FROM store_sales ch
            WHERE ch.store_id = s.id
              AND ch.store_id IN (1, 3, 4, 6)
        )
),
latest_match AS (
    -- Latest matching order per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.region,
        s.sqft,
        s.running_max_sqft,
        s.running_count_sqft,
        x.status
    FROM eligible_stores_2 s
    LEFT JOIN LATERAL (
            SELECT c.status
            FROM orders c
            WHERE c.store_id = s.id
            ORDER BY c.shipping_fee DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
scored AS (
    -- Ranking and gap features
    SELECT
        *,
        AVG(sqft) OVER w_ordered running_avg_sqft,
        COUNT(sqft) OVER w_ordered running_count_sqft_2,
        MIN(sqft) OVER w_ordered running_min_sqft,
        AVG(running_count_sqft) OVER (PARTITION BY region ORDER BY running_count_sqft ASC, id ASC ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) running_count_sqft_avg_3_prior
    FROM latest_match
    WINDOW w_ordered AS (PARTITION BY region ORDER BY running_count_sqft ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
)
-- Grouped metrics
SELECT
    status,
    MIN(running_count_sqft_avg_3_prior) AS min_running_count_sqft_avg_3_prior,
    COUNT(*) AS event_count,
    COUNT(CASE WHEN region = 'North' THEN 1 END) AS north_count,
    PERCENTILE_APPROX(running_max_sqft, ARRAY(0.25, 0.5, 0.75)) AS running_max_sqft_quartiles,
    COUNT(DISTINCT id) AS distinct_ids
FROM scored
GROUP BY CUBE (status)
ORDER BY north_count DESC
