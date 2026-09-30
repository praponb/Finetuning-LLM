SELECT s.country, MIN(p.launch_date) AS min_launch_date, MAX(p.cost) - MIN(p.cost) AS cost_range
FROM products p
INNER JOIN suppliers s ON p.supplier_id = s.id
GROUP BY s.country
ORDER BY min_launch_date DESC, 1
