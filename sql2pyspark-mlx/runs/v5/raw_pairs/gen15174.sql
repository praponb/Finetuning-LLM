SELECT CASE WHEN c.credit_limit >= 14800 THEN 'high' WHEN c.credit_limit >= 3000 THEN 'mid' ELSE 'low' END AS credit_limit_bucket, ROUND(STDDEV(o.amount), 2) AS stddev_amount, COUNT(DISTINCT o.shipping_fee) AS unique_customers
FROM orders o
LEFT JOIN customers c ON o.customer_id = c.id
GROUP BY CASE WHEN c.credit_limit >= 14800 THEN 'high' WHEN c.credit_limit >= 3000 THEN 'mid' ELSE 'low' END
ORDER BY unique_customers DESC, credit_limit_bucket
LIMIT 20
