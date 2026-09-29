WITH cleaned AS (
    SELECT *, COALESCE(brand, 'Unknown') AS brand_filled
    FROM products
    WHERE supplier_id IS NOT NULL
      AND ARRAY_CONTAINS(tags, 'premium')
),
with_latest AS (
    SELECT
        s.*,
        top1.order_item_quantity,
        top1.order_item_unit_price
    FROM cleaned s
    LEFT JOIN LATERAL (
            SELECT c.quantity AS order_item_quantity, c.unit_price AS order_item_unit_price
            FROM order_items c
            WHERE c.product_id = s.id
            ORDER BY c.unit_price DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
kept AS (
    SELECT s.*
    FROM with_latest s
    WHERE EXISTS (
            SELECT 1
            FROM store_sales x
            WHERE x.product_id = s.id
              AND x.sale_date >= s.launch_date
        )
),
with_last AS (
    SELECT
        s.id,
        s.product_name,
        s.category,
        s.brand,
        s.price,
        s.stock_qty,
        s.launch_date,
        s.tags,
        s.brand_filled,
        s.order_item_quantity,
        s.order_item_unit_price,
        top1.order_item_unit_price_2,
        top1.quantity
    FROM kept s
    LEFT JOIN LATERAL (
            SELECT c.unit_price AS order_item_unit_price_2, c.quantity
            FROM order_items c
            WHERE c.product_id = s.id
            ORDER BY c.unit_price DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
)
SELECT
    id,
    category,
    brand,
    price,
    launch_date,
    order_item_quantity,
    order_item_unit_price,
    CASE WHEN category RLIKE '^(Sports|Home)$' THEN 'GROUP_A' ELSE 'CORE' END AS category_zone
FROM with_last
ORDER BY order_item_unit_price DESC
