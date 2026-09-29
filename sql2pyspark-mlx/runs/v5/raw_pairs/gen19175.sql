SELECT * FROM (SELECT country, city, credit_limit FROM customers WHERE signup_date <= '2023-07-01')
PIVOT (COUNT(credit_limit) FOR city IN ('Berlin', 'New York'))
ORDER BY country
