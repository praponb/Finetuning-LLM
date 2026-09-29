SELECT * FROM (SELECT id, store_id, quantity, last_restock, DENSE_RANK() OVER (PARTITION BY store_id ORDER BY quantity DESC) AS rn FROM inventory) AS r WHERE rn = 1;
