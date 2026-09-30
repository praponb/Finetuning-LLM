SELECT category, MAX(cost) max_cost FROM products WHERE MONTH(launch_date) IN (6, 10, 11) AND (brand = 'Umbrella' OR launch_date >= '2025-05-01') GROUP BY category HAVING MAX(cost) >= 520
