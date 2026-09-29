WITH base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        CONCAT(category, ' ', product_name) AS category_product_name,
        QUARTER(launch_date) AS launch_date_quarter,
        COALESCE(brand, 'NONE') AS brand_filled
    FROM products
    WHERE launch_date <= '2024-08-01'
),
with_last AS (
    -- Latest matching order_item per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.category,
        s.brand,
        s.price,
        s.stock_qty,
        s.tags,
        s.supplier_id,
        s.category_product_name,
        top1.unit_price,
        top1.order_item_quantity
    FROM base s
    LEFT JOIN LATERAL (
            SELECT c.unit_price, c.quantity AS order_item_quantity
            FROM order_items c
            WHERE c.product_id = s.id
            ORDER BY c.unit_price DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
eligible AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM with_last s
    WHERE NOT EXISTS (
            SELECT 1
            FROM inventory x
            WHERE x.product_id = s.id
              AND x.last_restock BETWEEN '2024-09-01' AND '2025-05-01'
        )
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        id,
        stock_qty,
        tags,
        supplier_id,
        category_product_name,
        unit_price,
        order_item_quantity,
        LEAD(stock_qty, 1) OVER (PARTITION BY category ORDER BY stock_qty, id) AS next_stock_qty,
        LAST_VALUE(unit_price) IGNORE NULLS OVER (ORDER BY stock_qty, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_unit_price
    FROM eligible
)
-- Multi-level aggregation
SELECT
    supplier_id,
    COUNT(DISTINCT supplier_id) AS distinct_supplier_ids,
    BIT_AND(stock_qty) AS stock_qty_bit_and
FROM metrics
GROUP BY supplier_id
ORDER BY stock_qty_bit_and DESC NULLS FIRST;
