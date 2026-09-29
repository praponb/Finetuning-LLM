SELECT CASE WHEN c.age >= 61 THEN 'high' WHEN c.age >= 33 THEN 'mid' ELSE 'low' END AS age_bucket, MIN(o.order_date) AS first_order_date
FROM orders o
LEFT JOIN customers c ON c.id = o.customer_id
GROUP BY CASE WHEN c.age >= 61 THEN 'high' WHEN c.age >= 33 THEN 'mid' ELSE 'low' END
ORDER BY first_order_date;
