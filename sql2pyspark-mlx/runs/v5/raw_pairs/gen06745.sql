WITH base AS (
    -- Keep recent, valid rows
    SELECT *, cost - price margin
    FROM products
    WHERE brand IS NOT NULL
),
kept AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM base s
    WHERE EXISTS (
            SELECT 1
            FROM order_items ch
            WHERE ch.product_id = s.id
              AND ch.product_id = 13
        )
),
kept_keys AS (
    -- Set intersection on keys
    SELECT supplier_id
    FROM kept
    INTERSECT
    SELECT supplier_id
    FROM products
    WHERE SIZE(tags) > 1
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM kept s
    INNER JOIN kept_keys k ON s.supplier_id = k.supplier_id
),
with_dims AS (
    -- Attach suppliers attributes
    SELECT src.*, d.rating
    FROM kept_rows src
    LEFT JOIN suppliers d
        ON src.supplier_id = d.id
),
with_windows AS (
    -- Running and moving aggregates
    SELECT *, CUME_DIST() OVER (ORDER BY rating DESC NULLS FIRST) rating_cume_dist
    FROM with_dims
),
scored AS (
    -- Running and moving aggregates
    SELECT
        *,
        PERCENT_RANK() OVER (ORDER BY cost DESC NULLS FIRST) cost_percent_rank,
        CORR(cost, price) OVER (PARTITION BY supplier_id) corr_cost_price,
        CUME_DIST() OVER (ORDER BY rating_cume_dist DESC) rating_cume_dist_cume_dist,
        PERCENT_RANK() OVER (ORDER BY margin NULLS FIRST) margin_percent_rank
    FROM with_windows
),
cleaned AS (
    -- Basic cleansing and defaults
    SELECT *, rating * price rating_times_price
    FROM scored
    WHERE launch_date >= DATE '2023-06-28'
)
SELECT
    id,
    product_name,
    category,
    brand,
    price,
    stock_qty,
    launch_date,
    tags,
    rating_cume_dist,
    CASE WHEN category RLIKE '^(Electronics|Books)$' THEN 'GROUP_A' WHEN category RLIKE '^(Home|Sports)$' THEN 'CORE' ELSE 'PRIORITY' END AS category_zone
FROM cleaned
ORDER BY stock_qty DESC, category
