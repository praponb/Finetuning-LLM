SELECT
    s.id,
    s.product_name,
    s.category,
    s.price,
    s.cost,
    s.launch_date,
    s.tags,
    s.supplier_id,
    latest.order_item_unit_price,
    latest.quantity
FROM products s
LEFT JOIN LATERAL (
        SELECT c.unit_price AS order_item_unit_price, c.quantity
        FROM order_items c
        WHERE c.product_id = s.id
        ORDER BY c.unit_price DESC, c.id DESC
        LIMIT 1
    ) latest ON TRUE;
