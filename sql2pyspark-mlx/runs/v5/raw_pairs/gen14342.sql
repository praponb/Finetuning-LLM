SELECT
    order_items.order_id,
    order_items.quantity,
    order_items.unit_price,
    orders.shipping_fee,
    orders.discount,
    orders.store_id
FROM order_items
INNER JOIN orders ON order_items.order_id = orders.id
WHERE order_items.unit_price >= 250
ORDER BY orders.store_id DESC;
