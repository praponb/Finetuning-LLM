SELECT SUM(age) AS total_age, ROUND(AVG(credit_limit), 2) AS avg_credit_limit
FROM customers
WHERE (name LIKE 'Al%' AND country IN ('DE', 'US')) OR segment = 'Corporate'
