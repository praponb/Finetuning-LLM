SELECT
    order_items.id,
    order_items.unit_price,
    products.supplier_id,
    products.brand,
    orders.amount,
    orders.shipping_fee
FROM order_items
LEFT JOIN products ON order_items.product_id = products.id
INNER JOIN orders ON orders.id = order_items.order_id
ORDER BY amount DESC, id
LIMIT 20;
