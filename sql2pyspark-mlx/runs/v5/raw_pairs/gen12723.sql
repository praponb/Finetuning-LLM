SELECT store_id, COUNT(quantity) AS quantity_count, COUNT(*) AS inventory_count FROM inventory WHERE last_restock < '2024-08-01' AND product_id = 5 GROUP BY store_id ORDER BY inventory_count DESC;
