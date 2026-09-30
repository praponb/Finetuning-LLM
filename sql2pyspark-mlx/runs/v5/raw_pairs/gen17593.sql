SELECT stores.id, COUNT(inventory.id) AS inventory_count
FROM stores
LEFT JOIN inventory ON inventory.store_id = stores.id
WHERE stores.opened_date >= '2024-06-01'
GROUP BY stores.id
HAVING COUNT(inventory.id) >= 2
