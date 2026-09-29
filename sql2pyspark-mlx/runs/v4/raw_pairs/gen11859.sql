SELECT id, last_restock, quantity, quantity - LAG(quantity, 1) OVER (ORDER BY last_restock, id) AS quantity_change FROM inventory WHERE quantity = 133;
