SELECT id, last_restock, quantity, LEAD(quantity, 2) OVER (ORDER BY last_restock, id) AS next_quantity FROM inventory;
