SELECT YEAR(o.order_date) AS order_year, MAX(s.cost) - MIN(s.cost) AS cost_range FROM shipments s INNER JOIN orders o ON o.id = s.order_id GROUP BY YEAR(o.order_date) HAVING cost_range >= 30;
