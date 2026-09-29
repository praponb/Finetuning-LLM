SELECT DATE_FORMAT(order_date, 'yyyy-MM') AS order_month, ROUND(STDDEV(amount), 2) AS stddev_amount FROM orders WHERE MONTH(order_date) IN (5, 8, 12) GROUP BY DATE_FORMAT(order_date, 'yyyy-MM')
