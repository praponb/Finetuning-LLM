SELECT
    CASE WHEN p.cost >= 730 THEN 'high' WHEN p.cost >= 120 THEN 'mid' ELSE 'low' END AS cost_bucket,
    CASE WHEN YEAR(p.launch_date) < 2024 THEN 'before_2024' WHEN YEAR(p.launch_date) = 2024 THEN '2024' ELSE 'after_2024' END AS launch_cohort,
    MIN(i.last_restock) AS min_last_restock,
    COUNT(*) AS total_count
FROM inventory i
LEFT OUTER JOIN products p ON i.product_id = p.id
INNER JOIN suppliers s ON p.supplier_id = s.id
GROUP BY CASE WHEN p.cost >= 730 THEN 'high' WHEN p.cost >= 120 THEN 'mid' ELSE 'low' END, CASE WHEN YEAR(p.launch_date) < 2024 THEN 'before_2024' WHEN YEAR(p.launch_date) = 2024 THEN '2024' ELSE 'after_2024' END
ORDER BY launch_cohort;
