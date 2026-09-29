WITH with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        COUNT(sqft) OVER (PARTITION BY region ORDER BY sqft, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS last7_count_sqft,
        MAX(sqft) OVER (PARTITION BY region ORDER BY sqft, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS last7_max_sqft,
        AVG(sqft) OVER (PARTITION BY region ORDER BY sqft, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS last7_avg_sqft,
        SUM(sqft) OVER (PARTITION BY region ORDER BY sqft, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS last7_sum_sqft
    FROM stores
),
valid AS (
    -- Keep recent, valid rows
    SELECT
        *,
        COALESCE(store_name, 'Unknown') AS store_name_filled,
        COALESCE(region, 'N/A') AS region_clean
    FROM with_windows
    WHERE region IS NOT NULL
),
latest AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY city ORDER BY last7_sum_sqft DESC NULLS LAST, id) AS latest_rank
        FROM valid
    ) x
    WHERE latest_rank = 1
),
eligible AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM latest s
    WHERE NOT EXISTS (
            SELECT 1
            FROM inventory ch
            WHERE ch.store_id = s.id
              AND ch.last_restock >= s.opened_date
        )
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        LAG(last7_avg_sqft) OVER (PARTITION BY region, city ORDER BY last7_sum_sqft, id) AS prev_last7_avg_sqft,
        LEAD(last7_avg_sqft) OVER (PARTITION BY region, city ORDER BY last7_sum_sqft, id) AS next_last7_avg_sqft,
        LAG(sqft) OVER (PARTITION BY region, city ORDER BY last7_sum_sqft, id) AS prev_sqft,
        ROW_NUMBER() OVER (PARTITION BY region, city ORDER BY last7_sum_sqft, id) AS row_seq
    FROM eligible
),
eligible_stores AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM metrics s
    WHERE NOT EXISTS (
            SELECT 1
            FROM orders ch
            WHERE ch.store_id = s.id
              AND ch.order_date <= s.opened_date
        )
)
-- Native SQL PIVOT
SELECT *
FROM (
    SELECT
        region,
        city,
        prev_sqft
    FROM eligible_stores
) src
PIVOT (
    SUM(prev_sqft) AS total,
    COUNT(1) AS cnt
    FOR city IN ('Berlin' AS berlin, 'New York' AS new_york, 'Singapore' AS singapore)
);
