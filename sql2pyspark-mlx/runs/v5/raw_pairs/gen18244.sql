SELECT MONTH(order_date) order_month, QUARTER(order_date) order_quarter, COUNT(CASE WHEN channel = 'web' THEN 1 END) web_count
FROM orders
WHERE channel = 'store' AND discount > 0.38
GROUP BY order_month, order_quarter
HAVING COUNT(CASE WHEN channel = 'web' THEN 1 END) < 2
ORDER BY order_quarter DESC;
