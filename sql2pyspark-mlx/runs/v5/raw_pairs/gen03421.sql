SELECT order_date
FROM orders
WHERE discount IS NULL
EXCEPT
SELECT opened_date AS order_date
FROM accounts
