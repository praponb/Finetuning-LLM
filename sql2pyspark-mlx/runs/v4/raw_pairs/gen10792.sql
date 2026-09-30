WITH with_latest AS (
    -- Latest matching order_item per row (LATERAL ... LIMIT 1)
    SELECT
        s.*,
        top1.order_item_quantity,
        top1.unit_price
    FROM products s
    JOIN LATERAL (
            SELECT c.quantity AS order_item_quantity, c.unit_price
            FROM order_items c
            WHERE c.product_id = s.id
            ORDER BY c.quantity DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
)
SELECT
    s.id,
    s.product_name,
    s.category,
    s.brand,
    s.price,
    s.stock_qty,
    s.launch_date,
    s.tags,
    s.supplier_id,
    CASE WHEN category RLIKE '^(Books|Home)$' THEN 'PRIORITY' WHEN category RLIKE '^(Electronics|Sports)$' THEN 'GROUP_A' ELSE 'GROUP_B' END AS category_zone,
    (SELECT MIN(p2.price) FROM products p2 WHERE p2.supplier_id = s.supplier_id) AS min_product_price
FROM with_latest s
ORDER BY supplier_id, price DESC
