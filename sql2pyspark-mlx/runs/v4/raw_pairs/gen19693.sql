SELECT signup_date
FROM customers
EXCEPT
SELECT opened_date AS signup_date
FROM accounts;
