WITH numbered AS (SELECT id, product_id, quantity, last_restock, ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY quantity DESC, id) pos FROM inventory) SELECT * FROM numbered WHERE pos = 1;
