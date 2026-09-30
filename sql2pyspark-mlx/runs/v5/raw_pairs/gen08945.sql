SELECT
    s.id,
    s.store_id,
    s.order_date,
    s.amount,
    s.shipping_fee,
    latest.order_item_quantity
FROM orders s
LEFT JOIN LATERAL (
        SELECT c.quantity AS order_item_quantity
        FROM order_items c
        WHERE c.order_id = s.id
        ORDER BY c.unit_price DESC, c.id DESC
        LIMIT 1
    ) latest ON TRUE;
