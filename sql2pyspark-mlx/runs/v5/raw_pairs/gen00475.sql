SELECT
    products.id,
    products.category,
    t.quantity_count,
    t.unique_products
FROM products
INNER JOIN (SELECT product_id, COUNT(quantity) quantity_count, COUNT(DISTINCT unit_price) unique_products
    FROM order_items
    WHERE order_id IN (4, 40)
    GROUP BY product_id) t ON products.id = t.product_id
WHERE t.unique_products > 8
ORDER BY products.category ASC;
