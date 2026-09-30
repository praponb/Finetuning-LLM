SELECT * FROM (SELECT city, country, credit_limit FROM customers)
PIVOT (COUNT(credit_limit) FOR country IN ('DE', 'UK', 'SG'))
ORDER BY city
