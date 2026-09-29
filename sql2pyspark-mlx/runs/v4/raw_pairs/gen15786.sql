WITH ranked AS (SELECT *, ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY last_restock, id) AS pos FROM inventory) SELECT id, product_id, last_restock FROM ranked WHERE pos <= 5;
