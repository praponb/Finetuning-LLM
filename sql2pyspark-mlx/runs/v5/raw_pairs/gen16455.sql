WITH valid AS (
    -- Keep recent, valid rows
    SELECT
        id,
        store_name,
        city,
        region,
        opened_date,
        sqft,
        ROUND(sqft, 1) AS sqft_rounded,
        COALESCE(region, 'DIRECT') AS region_filled
    FROM stores
    WHERE region IS NOT NULL
),
eligible AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM valid s
    WHERE NOT EXISTS (
            SELECT 1
            FROM store_sales x
            WHERE x.store_id = s.id
              AND x.sale_date < s.opened_date
        )
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        id,
        store_name,
        region,
        sqft_rounded,
        COUNT(sqft) OVER w_grp AS running_count_sqft,
        MAX(sqft) OVER w_grp AS running_max_sqft,
        MIN(sqft) OVER w_grp AS running_min_sqft,
        AVG(sqft) OVER (ORDER BY sqft_rounded ASC, id ASC ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS sqft_avg_6_prior
    FROM eligible
    WHERE YEAR(opened_date) = 2023
    WINDOW w_grp AS (PARTITION BY region ORDER BY opened_date ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
kept AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM with_windows s
    WHERE NOT EXISTS (
            SELECT 1
            FROM store_sales x
            WHERE x.store_id = s.id
              AND x.store_id IN (1, 2, 3, 5)
        )
),
with_last AS (
    -- Latest matching inventory per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.region,
        s.sqft_rounded,
        s.running_min_sqft,
        top1.last_restock
    FROM kept s
    LEFT JOIN LATERAL (
            SELECT c.last_restock
            FROM inventory c
            WHERE c.store_id = s.id
            ORDER BY c.quantity DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
)
-- Aggregate with conditional metrics
SELECT
    DATE_TRUNC('WEEK', last_restock) AS week_start,
    SUM(sqft_rounded) AS total_sqft_rounded,
    ROUND(SUM(sqft_rounded) / NULLIF(COUNT(CASE WHEN region = 'East' THEN 1 END), 0), 2) AS sqft_rounded_per_east
FROM with_last
GROUP BY DATE_TRUNC('WEEK', last_restock)
ORDER BY total_sqft_rounded DESC
