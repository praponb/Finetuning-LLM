SELECT last_restock FROM inventory UNION SELECT order_date AS last_restock FROM orders ORDER BY last_restock
