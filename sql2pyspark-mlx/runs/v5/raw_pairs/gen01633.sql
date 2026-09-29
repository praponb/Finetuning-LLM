SELECT CASE WHEN YEAR(p.launch_date) < 2024 THEN 'before_2024' WHEN YEAR(p.launch_date) = 2024 THEN '2024' ELSE 'after_2024' END AS launch_cohort, MAX(i.last_restock) AS latest_last_restock, ROUND(AVG(i.quantity), 2) AS avg_quantity
FROM inventory i
INNER JOIN products p ON i.product_id = p.id
INNER JOIN stores s ON i.store_id = s.id
GROUP BY CASE WHEN YEAR(p.launch_date) < 2024 THEN 'before_2024' WHEN YEAR(p.launch_date) = 2024 THEN '2024' ELSE 'after_2024' END;
