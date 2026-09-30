SELECT MONTH(stores.opened_date) AS opened_month, AVG(inventory.quantity) AS avg_quantity
FROM inventory
JOIN products ON inventory.product_id = products.id
LEFT JOIN stores ON stores.id = inventory.store_id
WHERE inventory.product_id IN (1, 2, 4, 8)
GROUP BY MONTH(stores.opened_date)
HAVING AVG(inventory.quantity) >= 230;
