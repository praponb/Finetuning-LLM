SELECT store_id, last_restock FROM inventory WHERE store_id IN (3, 4) UNION SELECT store_id, last_restock FROM inventory WHERE product_id IN (3, 4, 5) ORDER BY last_restock DESC
