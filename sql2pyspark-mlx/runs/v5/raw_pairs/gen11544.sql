SELECT
    p.brand,
    COUNT(oi.id) AS order_item_count,
    MIN(oi.quantity) AS min_quantity,
    COUNT(DISTINCT oi.unit_price) AS unique_products
FROM products p
JOIN order_items oi ON oi.product_id = p.id
GROUP BY p.brand
