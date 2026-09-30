SELECT
    order_items.id,
    order_items.product_id,
    products.product_name,
    suppliers.rating,
    suppliers.supplier_name
FROM order_items
JOIN products ON products.id = order_items.product_id
JOIN suppliers ON products.supplier_id = suppliers.id;
