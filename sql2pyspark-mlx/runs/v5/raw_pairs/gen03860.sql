SELECT CASE WHEN amount >= 1640 THEN 'high' WHEN amount >= 370 THEN 'mid' ELSE 'low' END AS amount_bucket, DATE_FORMAT(order_date, 'yyyy-MM') AS order_month, AVG(amount) AS avg_amount
FROM orders
GROUP BY amount_bucket, order_month
HAVING AVG(amount) > 680
ORDER BY 1 DESC
