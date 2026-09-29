SELECT status, ROUND(STDDEV(cost), 2) AS stddev_cost FROM shipments WHERE delivered_date <= '2024-02-22' GROUP BY status HAVING ROUND(STDDEV(cost), 2) < 29 ORDER BY stddev_cost DESC
