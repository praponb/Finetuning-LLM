SELECT * FROM (SELECT id, product_id, last_restock, ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY last_restock, id) AS pos FROM inventory) AS r WHERE pos = 1 ORDER BY product_id DESC, id DESC
