SELECT
    products.category,
    products.product_name,
    t.min_last_restock,
    t.total_quantity
FROM products
INNER JOIN (SELECT product_id, MIN(last_restock) min_last_restock, SUM(quantity) total_quantity
    FROM inventory
    WHERE store_id IN (2, 5)
    GROUP BY product_id) t ON products.id = t.product_id
WHERE t.total_quantity > 550
