SELECT YEAR(c.signup_date) AS signup_year, ROUND(STDDEV(s.cost), 2) AS stddev_cost
FROM shipments s
INNER JOIN orders o ON o.id = s.order_id
INNER JOIN customers c ON o.customer_id = c.id
WHERE o.amount <= 1090
GROUP BY YEAR(c.signup_date)
ORDER BY stddev_cost DESC;
