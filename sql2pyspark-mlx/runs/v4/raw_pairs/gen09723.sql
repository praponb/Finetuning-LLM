SELECT id, product_id, last_restock, RANK() OVER (PARTITION BY product_id ORDER BY last_restock) AS rnk FROM inventory WHERE product_id IN (1, 3, 6, 8);
