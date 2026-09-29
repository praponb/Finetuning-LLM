SELECT CASE WHEN c.credit_limit >= 17700 THEN 'high' WHEN c.credit_limit >= 5400 THEN 'mid' ELSE 'low' END AS credit_limit_bucket, AVG(oi.unit_price) AS avg_unit_price, ROUND(STDDEV(oi.unit_price), 2) AS stddev_unit_price
FROM order_items oi
JOIN orders o ON oi.order_id = o.id
LEFT OUTER JOIN customers c ON c.id = o.customer_id
GROUP BY CASE WHEN c.credit_limit >= 17700 THEN 'high' WHEN c.credit_limit >= 5400 THEN 'mid' ELSE 'low' END
