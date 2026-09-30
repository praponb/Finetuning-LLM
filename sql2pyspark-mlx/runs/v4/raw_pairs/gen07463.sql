SELECT
    products.id,
    products.product_name,
    COUNT(inventory.id) inventory_count,
    ROUND(AVG(inventory.quantity), 2) avg_quantity
FROM products
LEFT JOIN inventory ON inventory.product_id = products.id
WHERE SIZE(products.tags) > 2
GROUP BY products.id, products.product_name
HAVING COUNT(inventory.id) >= 2
ORDER BY inventory_count DESC;
