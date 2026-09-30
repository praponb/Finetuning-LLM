SELECT o.status, QUARTER(o.order_date) AS order_quarter, ROUND(STDDEV(s.cost), 2) AS stddev_cost
FROM shipments s
LEFT JOIN orders o ON s.order_id = o.id
GROUP BY o.status, QUARTER(o.order_date)
HAVING ROUND(STDDEV(s.cost), 2) > 54
ORDER BY status;
