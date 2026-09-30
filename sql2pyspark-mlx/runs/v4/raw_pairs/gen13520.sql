SELECT
    order_items.id,
    order_items.product_id,
    order_items.order_id,
    products.launch_date,
    suppliers.city,
    suppliers.rating
FROM order_items
INNER JOIN products ON order_items.product_id = products.id
INNER JOIN suppliers ON products.supplier_id = suppliers.id
ORDER BY rating DESC;
