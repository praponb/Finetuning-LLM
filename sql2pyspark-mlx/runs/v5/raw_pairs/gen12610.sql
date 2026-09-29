SELECT carrier, MONTH(shipped_date) AS shipped_month, status, ROUND(STDDEV(cost), 2) AS stddev_cost, COUNT(cost) AS cost_count FROM shipments GROUP BY 1, 2, 3 ORDER BY shipped_month DESC
