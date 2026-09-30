WITH base AS (
    -- Keep recent, valid rows
    SELECT *, ADD_MONTHS(launch_date, 1) AS launch_date_plus_months
    FROM products
    WHERE launch_date >= DATE '2024-02-22'
),
with_last AS (
    -- Latest matching order_item per row (LATERAL ... LIMIT 1)
    SELECT s.*, top1.order_item_quantity
    FROM base s
    LEFT JOIN LATERAL (
            SELECT c.quantity AS order_item_quantity
            FROM order_items c
            WHERE c.product_id = s.id
            ORDER BY c.unit_price DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
kept AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM with_last s
    WHERE NOT EXISTS (
            SELECT 1
            FROM store_sales ch
            WHERE ch.product_id = s.id
              AND ch.sale_date <= s.launch_date_plus_months
        )
)
SELECT
    id,
    product_name,
    category,
    stock_qty,
    supplier_id,
    launch_date_plus_months,
    order_item_quantity,
    CASE WHEN category RLIKE '^(Sports|Grocery)$' THEN 'GROUP_A' WHEN category RLIKE '^(Electronics|Home)$' THEN 'CORE' ELSE 'EXTENDED' END AS category_zone
FROM kept
ORDER BY category_zone DESC;
