SELECT
    order_items.quantity,
    products.cost,
    products.brand,
    suppliers.supplier_name,
    suppliers.rating
FROM order_items
LEFT OUTER JOIN products ON order_items.product_id = products.id
INNER JOIN suppliers ON products.supplier_id = suppliers.id;
