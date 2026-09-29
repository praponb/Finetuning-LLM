SELECT
    products.category,
    products.brand,
    AVG(inventory.quantity) AS avg_quantity,
    MAX(inventory.quantity) AS max_quantity
FROM inventory
INNER JOIN products ON inventory.product_id = products.id
INNER JOIN stores ON inventory.store_id = stores.id
WHERE inventory.store_id IN (1, 3, 5)
GROUP BY products.category, products.brand
ORDER BY category DESC, brand DESC
LIMIT 20
