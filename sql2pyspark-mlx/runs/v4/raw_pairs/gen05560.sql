SELECT shipped_date
FROM shipments
EXCEPT
SELECT signup_date AS shipped_date
FROM customers
