SELECT
    order_items.id,
    order_items.order_id,
    order_items.unit_price,
    products.launch_date
FROM order_items
LEFT OUTER JOIN products ON order_items.product_id = products.id;
