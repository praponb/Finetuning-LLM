SELECT opened_date
FROM accounts
EXCEPT
SELECT signup_date AS opened_date
FROM customers
ORDER BY opened_date;
