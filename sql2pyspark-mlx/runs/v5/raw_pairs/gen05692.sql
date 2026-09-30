SELECT id, segment
FROM customers
WHERE is_active = FALSE
UNION
SELECT id, segment
FROM customers
WHERE credit_limit BETWEEN 8800 AND 10000;
