SELECT age, email FROM customers WHERE NOT is_active UNION SELECT age, email FROM customers WHERE credit_limit >= 8600
