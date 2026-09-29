SELECT id, city, credit_limit, SUM(credit_limit) OVER (PARTITION BY city) AS city_sum_credit_limit FROM customers WHERE name LIKE 'Pi%';
