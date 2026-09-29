SELECT signup_date
FROM customers
UNION
SELECT shipped_date
FROM shipments
ORDER BY signup_date DESC;
