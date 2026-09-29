SELECT id, product_id, quantity, RANK() OVER (PARTITION BY product_id ORDER BY quantity) AS rnk FROM inventory;
