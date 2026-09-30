SELECT o.status, COUNT(DISTINCT s.cost) AS unique_orders FROM shipments s INNER JOIN orders o ON s.order_id = o.id GROUP BY o.status ORDER BY o.status ASC
