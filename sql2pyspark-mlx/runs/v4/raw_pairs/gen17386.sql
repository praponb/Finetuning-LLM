SELECT city
FROM customers
WHERE credit_limit > 9100
UNION ALL
SELECT city
FROM customers
WHERE is_active = FALSE;
