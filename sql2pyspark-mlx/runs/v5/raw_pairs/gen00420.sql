WITH ranked AS (SELECT *, RANK() OVER (PARTITION BY product_id ORDER BY quantity) AS rnk FROM inventory) SELECT id, product_id, quantity, store_id FROM ranked WHERE rnk <= 2 ORDER BY 1;
