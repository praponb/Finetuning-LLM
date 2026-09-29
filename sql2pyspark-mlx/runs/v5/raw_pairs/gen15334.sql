SELECT id, quantity FROM inventory WHERE quantity > 146 UNION SELECT id, quantity FROM inventory WHERE MONTH(last_restock) IN (4, 7, 8)
