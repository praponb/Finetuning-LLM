SELECT order_date
FROM orders
EXCEPT
SELECT signup_date AS order_date
FROM customers;
