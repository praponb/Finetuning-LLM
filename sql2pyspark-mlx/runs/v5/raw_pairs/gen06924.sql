SELECT id, product_id, last_restock FROM inventory WHERE (product_id IN (4, 10) AND quantity = 154) OR product_id IN (4, 9) ORDER BY last_restock
