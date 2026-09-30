SELECT DATE_TRUNC('MONTH', stores.opened_date) AS opened_month, COUNT(*) AS row_count, COUNT(*) AS num_inventory
FROM inventory
INNER JOIN stores ON stores.id = inventory.store_id
WHERE inventory.last_restock BETWEEN '2023-12-08' AND '2024-02-25'
GROUP BY DATE_TRUNC('MONTH', stores.opened_date)
HAVING COUNT(*) > 1
