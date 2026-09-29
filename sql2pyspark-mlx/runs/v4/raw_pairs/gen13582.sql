SELECT s.city, s.region, ROUND((100 * SUM(CASE WHEN o.channel = 'mobile' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_mobile
FROM orders o
INNER JOIN stores s ON s.id = o.store_id
INNER JOIN customers c ON o.customer_id = c.id
GROUP BY s.city, s.region;
