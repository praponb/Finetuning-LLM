SELECT category, brand, stddev_cost FROM (SELECT category, brand, ROUND(STDDEV(cost), 2) AS stddev_cost FROM products GROUP BY category, brand) AS s WHERE stddev_cost < 42 ORDER BY stddev_cost DESC
