WITH valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        CAST(rating AS INT) AS rating_int,
        ROUND(rating * 0.9, 2) AS adjusted_rating
    FROM suppliers
    WHERE city IN ('London', 'Chiang Mai')
),
cleaned AS (
    -- Keep recent, valid rows
    SELECT *, rating_int * adjusted_rating AS rating_int_times_adjusted_rating
    FROM valid
    WHERE city IS NOT NULL
),
with_latest AS (
    -- Latest matching product per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.supplier_name,
        s.city,
        s.rating,
        s.rating_int,
        s.adjusted_rating,
        s.rating_int_times_adjusted_rating,
        x.stock_qty,
        x.cost
    FROM cleaned s
    LEFT JOIN LATERAL (
            SELECT c.stock_qty, c.cost
            FROM products c
            WHERE c.supplier_id = s.id
            ORDER BY c.stock_qty DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
with_last AS (
    -- Latest matching product per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.supplier_name,
        s.rating,
        s.adjusted_rating,
        s.stock_qty,
        s.cost,
        latest.product_stock_qty
    FROM with_latest s
    LEFT JOIN LATERAL (
            SELECT c.stock_qty AS product_stock_qty
            FROM products c
            WHERE c.supplier_id = s.id
            ORDER BY c.cost DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        NTILE(10) OVER (ORDER BY rating DESC, id) AS rating_bucket,
        LAG(cost, 1, 0) OVER (ORDER BY product_stock_qty, id) AS prev_cost,
        FIRST_VALUE(adjusted_rating, TRUE) OVER (ORDER BY rating, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_adjusted_rating,
        SUM(product_stock_qty) OVER (ORDER BY stock_qty, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_product_stock_qty
    FROM with_last
),
eligible AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM with_windows s
    WHERE EXISTS (
            SELECT 1
            FROM products x
            WHERE x.supplier_id = s.id
              AND UPPER(x.product_name) LIKE '%OGA%'
        )
)
-- Grouped metrics
SELECT
    CASE WHEN first_known_adjusted_rating >= 84 THEN 'LARGE' ELSE 'SMALL' END AS first_known_adjusted_rating_tier,
    CASE WHEN cost >= 790 THEN 'HIGH' ELSE 'LOW' END AS cost_tier,
    ROUND(AVG(rating_bucket), 2) AS avg_rating_bucket,
    ROUND(AVG(rating_bucket), 2) AS avg_rating_bucket_2,
    COUNT(DISTINCT id) AS distinct_ids,
    COUNT(DISTINCT id) AS distinct_ids_2
FROM eligible
GROUP BY GROUPING SETS ((CASE WHEN first_known_adjusted_rating >= 84 THEN 'LARGE' ELSE 'SMALL' END, CASE WHEN cost >= 790 THEN 'HIGH' ELSE 'LOW' END), (CASE WHEN first_known_adjusted_rating >= 84 THEN 'LARGE' ELSE 'SMALL' END), ())
HAVING avg_rating_bucket_2 < 1000000
ORDER BY cost_tier;
