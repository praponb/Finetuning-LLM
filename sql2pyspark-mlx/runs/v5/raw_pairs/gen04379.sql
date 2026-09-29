SELECT status, total_cost FROM (SELECT status, SUM(cost) AS total_cost FROM shipments WHERE carrier IN ('Kerry', 'DHL') GROUP BY status) AS t WHERE total_cost > 490 ORDER BY total_cost
