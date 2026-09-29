SELECT id, product_id, last_restock, ROUND(quantity, 1) AS quantity_rounded FROM inventory WHERE quantity <= 181 ORDER BY last_restock DESC, quantity_rounded, id LIMIT 10;
