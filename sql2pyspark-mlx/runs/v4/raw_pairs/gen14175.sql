SELECT
    order_items.id,
    order_items.quantity,
    order_items.order_id,
    products.brand,
    suppliers.city
FROM order_items
INNER JOIN products ON order_items.product_id = products.id
LEFT OUTER JOIN suppliers ON products.supplier_id = suppliers.id
ORDER BY order_id DESC
