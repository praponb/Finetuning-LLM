WITH kept AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM stores s
    WHERE NOT EXISTS (
            SELECT 1
            FROM inventory c
            WHERE c.store_id = s.id
              AND c.product_id IN (1, 5, 6, 8)
        )
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        *,
        MAX(sqft) OVER (PARTITION BY region, city ORDER BY sqft, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS last7_max_sqft,
        AVG(sqft) OVER (PARTITION BY region, city ORDER BY sqft, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS last7_avg_sqft,
        COUNT(sqft) OVER (PARTITION BY region, city ORDER BY sqft, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS last7_count_sqft
    FROM kept
),
latest_match AS (
    -- Latest matching inventory per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.city,
        s.region,
        s.sqft,
        latest.last_restock,
        latest.quantity
    FROM enriched s
    LEFT JOIN LATERAL (
            SELECT c.last_restock, c.quantity
            FROM inventory c
            WHERE c.store_id = s.id
            ORDER BY c.quantity DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        sqft * quantity AS sqft_times_quantity,
        ADD_MONTHS(last_restock, 1) AS last_restock_plus_months,
        COALESCE(region, 'UNKNOWN') AS region_clean
    FROM latest_match
    WHERE quantity > 153
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        AVG(sqft_times_quantity) OVER w_ordered AS running_avg_sqft_times_quantity,
        MAX(sqft_times_quantity) OVER w_ordered AS running_max_sqft_times_quantity,
        MIN(sqft_times_quantity) OVER w_ordered AS running_min_sqft_times_quantity,
        COUNT(sqft_times_quantity) OVER w_ordered AS running_count_sqft_times_quantity,
        NTILE(5) OVER (ORDER BY sqft_times_quantity DESC, id) AS sqft_times_quantity_bucket
    FROM filtered
    WINDOW w_ordered AS (PARTITION BY region ORDER BY sqft_times_quantity, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
)
SELECT
    id,
    quantity,
    last_restock_plus_months,
    region_clean,
    running_count_sqft_times_quantity,
    sqft_times_quantity_bucket
FROM metrics
ORDER BY last_restock_plus_months
