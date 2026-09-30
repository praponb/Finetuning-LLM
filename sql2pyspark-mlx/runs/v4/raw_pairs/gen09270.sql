SELECT method, COUNT(DISTINCT amount) AS unique_orders, ROUND(STDDEV(amount), 2) AS stddev_amount FROM payments GROUP BY method ORDER BY method DESC LIMIT 5;
