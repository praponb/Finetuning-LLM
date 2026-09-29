SELECT id, age
FROM customers
WHERE credit_limit < 7400
UNION ALL
SELECT id, age
FROM customers
WHERE NOT is_active
ORDER BY id;
