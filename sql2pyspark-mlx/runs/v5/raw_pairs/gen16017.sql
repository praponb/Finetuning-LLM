SELECT products.brand, products.category, MAX(inventory.quantity) - MIN(inventory.quantity) AS quantity_range
FROM inventory
RIGHT OUTER JOIN stores ON inventory.store_id = stores.id
INNER JOIN products ON inventory.product_id = products.id
GROUP BY products.brand, products.category
