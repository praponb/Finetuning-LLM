SELECT QUARTER(o.order_date) AS order_quarter, COUNT(s.cost) AS cost_count, COUNT(*) AS cnt
FROM shipments s
RIGHT JOIN orders o ON s.order_id = o.id
WHERE o.order_date < '2024-05-01'
GROUP BY QUARTER(o.order_date)
ORDER BY order_quarter ASC
LIMIT 1;
