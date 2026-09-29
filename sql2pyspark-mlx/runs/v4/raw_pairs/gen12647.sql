WITH valid AS (
    -- Keep recent, valid rows
    SELECT
        id,
        category,
        price,
        stock_qty,
        launch_date,
        supplier_id,
        price / NULLIF(stock_qty, 0) AS price_per_stock_qty
    FROM products
    WHERE launch_date >= DATE '2023-07-26'
),
scored AS (
    -- Running and moving aggregates
    SELECT
        id,
        category,
        price,
        stock_qty,
        launch_date,
        supplier_id,
        price_per_stock_qty,
        FIRST_VALUE(price_per_stock_qty) IGNORE NULLS OVER (PARTITION BY supplier_id ORDER BY stock_qty, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_price_per_stock_qty,
        NTILE(10) OVER (PARTITION BY category, supplier_id ORDER BY price_per_stock_qty, id) AS price_per_stock_qty_bucket
    FROM valid
),
not_flagged AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM scored s
    WHERE EXISTS (
            SELECT 1
            FROM inventory c
            WHERE c.product_id = s.id
              AND c.last_restock <= s.launch_date
        )
),
scored_products AS (
    -- Running and moving aggregates
    SELECT
        *,
        AVG(price_per_stock_qty) OVER (ORDER BY price_per_stock_qty, id ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS price_per_stock_qty_avg_5,
        LAG(price, 2) OVER (PARTITION BY category ORDER BY price_per_stock_qty_bucket, id) AS prev_price,
        NTILE(3) OVER (PARTITION BY category ORDER BY price DESC, id) AS price_bucket,
        CORR(stock_qty, price) OVER (PARTITION BY category) AS corr_stock_qty_price
    FROM not_flagged
)
SELECT
    id,
    category,
    price,
    launch_date,
    price_per_stock_qty,
    price_per_stock_qty_bucket,
    price_per_stock_qty_avg_5,
    prev_price,
    price_bucket,
    CASE WHEN category RLIKE '^(Fashion|Books)$' THEN 'CORE' WHEN category RLIKE '^(Electronics|Home)$' THEN 'OTHER_ZONE' ELSE 'GROUP_A' END AS category_zone
FROM scored_products
ORDER BY category_zone DESC NULLS FIRST, price_per_stock_qty
