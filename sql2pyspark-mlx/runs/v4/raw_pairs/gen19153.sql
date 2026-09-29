SELECT id, product_id, last_restock FROM inventory WHERE MONTH(last_restock) IN (1, 2, 4) UNION SELECT id, product_id, last_restock FROM inventory WHERE product_id IN (1, 6);
