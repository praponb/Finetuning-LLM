SELECT o.channel, YEAR(o.order_date) AS order_year, COUNT(DISTINCT s.cost) AS unique_carriers
FROM shipments s
INNER JOIN orders o ON o.id = s.order_id
GROUP BY o.channel, YEAR(o.order_date)
HAVING COUNT(DISTINCT s.cost) >= 4
