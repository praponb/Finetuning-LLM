SELECT customer_id, YEAR(order_date) AS order_date_year
FROM orders
ORDER BY customer_id, order_date_year DESC;
