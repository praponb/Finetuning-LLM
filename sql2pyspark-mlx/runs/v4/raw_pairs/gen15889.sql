SELECT id, product_id, last_restock FROM (SELECT *, DENSE_RANK() OVER (PARTITION BY product_id ORDER BY last_restock DESC) AS pos FROM inventory) AS ranked WHERE pos <= 5;
