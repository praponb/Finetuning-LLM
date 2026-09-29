SELECT id, product_id, last_restock FROM (SELECT *, RANK() OVER (PARTITION BY product_id ORDER BY quantity) AS row_num FROM inventory) t WHERE row_num = 1
