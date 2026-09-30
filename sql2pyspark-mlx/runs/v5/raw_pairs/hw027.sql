SELECT YEAR(order_date) AS yr, MONTH(order_date) AS mo, COUNT(*) AS order_count FROM orders GROUP BY YEAR(order_date), MONTH(order_date);
