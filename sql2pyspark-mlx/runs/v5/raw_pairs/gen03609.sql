SELECT stores.city, MAX(inventory.quantity) max_quantity
FROM inventory
LEFT JOIN products ON inventory.product_id = products.id
FULL OUTER JOIN stores ON inventory.store_id = stores.id
WHERE inventory.product_id = 6
GROUP BY stores.city
HAVING MAX(inventory.quantity) > 220
