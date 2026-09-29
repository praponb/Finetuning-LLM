SELECT id, credit_limit, age
FROM customers
WHERE credit_limit > (SELECT AVG(credit_limit) FROM customers)
