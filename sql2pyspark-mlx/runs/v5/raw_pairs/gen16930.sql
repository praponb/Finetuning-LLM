SELECT id, last_restock, quantity, LAG(quantity, 1) OVER (ORDER BY last_restock, id) AS prev_quantity FROM inventory WHERE quantity IS NULL
