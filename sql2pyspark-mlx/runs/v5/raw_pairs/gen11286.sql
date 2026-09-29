WITH base AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, CASE WHEN country IN ('DE', 'TH') THEN 'Tier 1' ELSE 'Other' END AS country_group
    FROM suppliers
    WHERE city IN ('London', 'New York')
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        AVG(rating) OVER (PARTITION BY city ORDER BY rating ASC, id ASC ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS rating_avg_3_prior,
        SUM(rating) OVER (ORDER BY rating ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_rating
    FROM base
),
not_flagged AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM metrics s
    WHERE NOT EXISTS (
            SELECT 1
            FROM products c
            WHERE c.supplier_id = s.id
              AND c.supplier_id IS NOT NULL
        )
),
kept AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM not_flagged s
    WHERE EXISTS (
            SELECT 1
            FROM products c
            WHERE c.supplier_id = s.id
              AND c.supplier_id IN (22, 30, 36, 37)
        )
),
kept_suppliers AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM kept s
    WHERE NOT EXISTS (
            SELECT 1
            FROM products c
            WHERE c.supplier_id = s.id
              AND c.supplier_id IS NOT NULL
        )
),
not_flagged_suppliers AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM kept_suppliers s
    WHERE NOT EXISTS (
            SELECT 1
            FROM products c
            WHERE c.supplier_id = s.id
              AND SIZE(c.tags) > 1
        )
),
with_last AS (
    -- Latest matching product per row (LATERAL ... LIMIT 1)
    SELECT s.*, top1.product_stock_qty
    FROM not_flagged_suppliers s
    LEFT JOIN LATERAL (
            SELECT c.stock_qty AS product_stock_qty
            FROM products c
            WHERE c.supplier_id = s.id
            ORDER BY c.launch_date DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
)
-- Multi-level aggregation
SELECT
    CASE WHEN rating_avg_3_prior >= 4 THEN 'LARGE' ELSE 'SMALL' END AS rating_avg_3_prior_tier,
    COUNT(*) FILTER (WHERE country = 'SG') AS sg_rows,
    ROUND(AVG(rating_avg_3_prior), 2) AS avg_rating_avg_3_prior
FROM with_last
GROUP BY rating_avg_3_prior_tier
ORDER BY avg_rating_avg_3_prior DESC;
