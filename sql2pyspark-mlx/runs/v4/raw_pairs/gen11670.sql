SELECT name, age
FROM customers
WHERE NOT (credit_limit >= 11300 AND age = 53)
