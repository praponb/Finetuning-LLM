SELECT opened_date
FROM accounts
INTERSECT
SELECT signup_date
FROM customers
ORDER BY opened_date DESC;
