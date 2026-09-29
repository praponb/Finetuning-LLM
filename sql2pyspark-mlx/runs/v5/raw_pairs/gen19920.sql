SELECT QUARTER(launch_date) AS launch_quarter, brand, ROUND(STDDEV(cost), 2) AS stddev_cost FROM products GROUP BY 1, 2 HAVING ROUND(STDDEV(cost), 2) > 49 ORDER BY brand
