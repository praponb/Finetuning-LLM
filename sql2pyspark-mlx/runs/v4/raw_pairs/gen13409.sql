SELECT YEAR(o.order_date) order_year, COUNT(DISTINCT s.cost) unique_statuss FROM shipments s JOIN orders o ON s.order_id = o.id GROUP BY YEAR(o.order_date) ORDER BY order_year;
