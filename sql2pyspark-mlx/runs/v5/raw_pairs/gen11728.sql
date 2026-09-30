SELECT id, product_id, last_restock, DENSE_RANK() OVER (PARTITION BY product_id ORDER BY last_restock DESC) AS dense_rnk FROM inventory WHERE quantity <= 205;
