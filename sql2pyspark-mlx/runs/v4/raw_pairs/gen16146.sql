SELECT id, quantity, ROUND(quantity * 1.07, 2) AS quantity_scaled FROM inventory WHERE last_restock >= '2023-08-31' AND store_id IN (2, 5);
