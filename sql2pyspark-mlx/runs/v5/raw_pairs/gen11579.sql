SELECT QUARTER(launch_date) AS launch_quarter, category, ROUND(AVG(cost), 2) AS avg_cost FROM products GROUP BY QUARTER(launch_date), category ORDER BY category ASC, launch_quarter ASC LIMIT 5;
