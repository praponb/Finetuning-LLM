SELECT last_restock, quantity FROM inventory WHERE product_id IN (2, 9) AND MONTH(last_restock) IN (1, 8, 12) ORDER BY 2 DESC
