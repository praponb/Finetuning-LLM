SELECT is_active, COUNT(DISTINCT credit_limit) AS unique_citys FROM customers WHERE signup_date <= '2024-05-01' GROUP BY is_active HAVING COUNT(DISTINCT credit_limit) < 2 ORDER BY unique_citys DESC
