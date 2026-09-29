SELECT brand, ROUND(STDDEV(cost), 2) AS stddev_cost FROM products GROUP BY brand
