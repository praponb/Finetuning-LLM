SELECT id, segment
FROM customers
WHERE email IS NOT NULL
EXCEPT
SELECT id, segment
FROM customers
WHERE credit_limit BETWEEN 10000 AND 10800;
