SELECT store_id, SUM(quantity) total_quantity, MIN(last_restock) first_last_restock
FROM inventory
GROUP BY store_id;
