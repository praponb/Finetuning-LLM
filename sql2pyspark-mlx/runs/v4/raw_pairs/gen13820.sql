SELECT MONTH(order_date) AS order_month, QUARTER(order_date) AS order_quarter, SUM(CASE WHEN channel = 'store' THEN 1 ELSE 0 END) AS store_count
FROM orders
GROUP BY MONTH(order_date), QUARTER(order_date)
HAVING store_count < 24
ORDER BY order_quarter DESC, order_month
