WITH cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        ROUND(sqft, 1) AS sqft_rounded,
        CASE WHEN city = 'New York' THEN 1 ELSE 0 END AS is_new_york,
        COALESCE(region, 'N/A') AS resolved_region
    FROM stores
    WHERE region NOT IN ('South', 'West', 'Central')
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        id,
        opened_date,
        sqft_rounded,
        resolved_region,
        COUNT(sqft) OVER (PARTITION BY region ORDER BY is_new_york, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS last4_count_sqft,
        MIN(sqft) OVER (PARTITION BY region ORDER BY is_new_york, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS last4_min_sqft,
        MAX(sqft) OVER (PARTITION BY region ORDER BY is_new_york, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS last4_max_sqft,
        SUM(sqft) OVER (PARTITION BY city ORDER BY sqft, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_sqft
    FROM cleaned
),
with_last AS (
    -- Latest matching inventory per row (LATERAL ... LIMIT 1)
    SELECT
        s.*,
        top1.inventory_quantity,
        top1.inventory_last_restock
    FROM with_windows s
    JOIN LATERAL (
            SELECT c.quantity AS inventory_quantity, c.last_restock AS inventory_last_restock
            FROM inventory c
            WHERE c.store_id = s.id
            ORDER BY c.quantity DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
kept AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM with_last s
    WHERE EXISTS (
            SELECT 1
            FROM inventory x
            WHERE x.store_id = s.id
              AND x.last_restock <= s.opened_date
        )
),
filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        sqft_rounded,
        resolved_region,
        last4_count_sqft,
        cumulative_sqft,
        inventory_quantity,
        inventory_last_restock,
        CAST(last4_max_sqft AS INT) AS last4_max_sqft_int
    FROM kept
    WHERE cumulative_sqft >= 64070
)
-- Running and moving aggregates
SELECT
    id,
    sqft_rounded,
    resolved_region,
    inventory_quantity,
    inventory_last_restock,
    LAST_VALUE(cumulative_sqft, TRUE) OVER w_grp AS last_known_cumulative_sqft,
    SUM(sqft_rounded) OVER (ORDER BY sqft_rounded, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_sqft_rounded,
    RANK() OVER (ORDER BY last4_count_sqft) AS last4_count_sqft_rank,
    LEAD(cumulative_sqft, 2) OVER w_running AS next_cumulative_sqft
FROM filtered
WINDOW w_grp AS (ORDER BY inventory_quantity, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), w_running AS (ORDER BY inventory_last_restock, id)
