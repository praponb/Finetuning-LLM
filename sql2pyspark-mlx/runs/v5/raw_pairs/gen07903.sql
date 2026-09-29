SELECT DATE_TRUNC('MONTH', order_date) AS order_month, COUNT(*) AS order_count, COUNT(*) AS num_orders
FROM orders
WHERE amount > 1450 OR discount BETWEEN 0.33 AND 0.34
GROUP BY DATE_TRUNC('MONTH', order_date)
HAVING COUNT(*) < 5
