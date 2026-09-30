SELECT opened_date
FROM stores
UNION
SELECT signup_date
FROM customers
ORDER BY opened_date DESC;
