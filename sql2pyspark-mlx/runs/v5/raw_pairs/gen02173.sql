SELECT DATE_FORMAT(launch_date, 'yyyy-MM') AS launch_month, SUM(cost) AS total_cost, ROUND(SUM(cost) * 100.0 / SUM(SUM(cost)) OVER (), 2) AS pct_of_total
FROM products
WHERE brand IN ('Umbrella', 'Globex')
GROUP BY DATE_FORMAT(launch_date, 'yyyy-MM');
