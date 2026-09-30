SELECT
    products.category,
    products.brand,
    products.supplier_id,
    order_items.product_id,
    order_items.unit_price,
    order_items.quantity
FROM products
JOIN order_items ON products.id = order_items.product_id
WHERE products.cost > 190 AND LENGTH(products.product_name) >= 6
ORDER BY order_items.quantity DESC
