SELECT last_restock FROM inventory WHERE MONTH(last_restock) IN (6, 8, 12) UNION SELECT signup_date AS last_restock FROM customers WHERE country IN ('DE', 'SG')
