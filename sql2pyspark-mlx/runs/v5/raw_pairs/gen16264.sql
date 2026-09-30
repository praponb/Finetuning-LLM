WITH base AS (
    SELECT
        id,
        product_name,
        category,
        brand,
        price,
        cost,
        stock_qty,
        launch_date,
        tags,
        supplier_id,
        CASE WHEN price >= 1070 THEN 'High' WHEN price >= 590 THEN 'Medium' ELSE 'Low' END AS price_tier,
        DATE_TRUNC('year', launch_date) AS launch_date_period
    FROM products
    WHERE supplier_id IS NOT NULL
      AND launch_date >= DATE '2023-08-02'
),
latest AS (
    SELECT *
    FROM base
    QUALIFY ROW_NUMBER() OVER (PARTITION BY category ORDER BY launch_date_period DESC, id) = 1
),
with_last AS (
    SELECT
        s.id,
        s.price,
        s.cost,
        s.launch_date,
        s.tags,
        s.price_tier,
        s.launch_date_period,
        top1.store_sale_sale_date,
        top1.revenue
    FROM latest s
    JOIN LATERAL (
            SELECT c.sale_date AS store_sale_sale_date, c.revenue
            FROM store_sales c
            WHERE c.product_id = s.id
            ORDER BY c.revenue DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
with_latest AS (
    SELECT s.*, x.units
    FROM with_last s
    LEFT JOIN LATERAL (
            SELECT c.units
            FROM store_sales c
            WHERE c.product_id = s.id
            ORDER BY c.revenue DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
kept AS (
    SELECT s.*
    FROM with_latest s
    WHERE EXISTS (
            SELECT 1
            FROM order_items x
            WHERE x.product_id = s.id
              AND x.quantity BETWEEN 4 AND 8
        )
)
SELECT
    id,
    cost,
    tags,
    price_tier,
    launch_date_period,
    store_sale_sale_date,
    revenue
FROM kept
ORDER BY launch_date_period DESC;
