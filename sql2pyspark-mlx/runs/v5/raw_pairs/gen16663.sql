SELECT s.shipped_date, s.order_id, s.carrier, o.channel FROM shipments s INNER JOIN orders o ON s.order_id = o.id WHERE o.channel != 'web' AND YEAR(o.order_date) = 2023 ORDER BY s.shipped_date ASC
