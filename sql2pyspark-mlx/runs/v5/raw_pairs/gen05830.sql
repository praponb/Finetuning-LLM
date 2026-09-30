WITH ranked AS (SELECT *, ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY last_restock, id) AS row_num FROM inventory) SELECT * FROM ranked WHERE row_num <= 5;
