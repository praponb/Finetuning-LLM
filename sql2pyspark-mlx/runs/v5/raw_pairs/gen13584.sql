SELECT CASE WHEN YEAR(p.launch_date) < 2024 THEN 'before_2024' WHEN YEAR(p.launch_date) = 2024 THEN '2024' ELSE 'after_2024' END launch_cohort, SUM(ss.revenue) total_revenue
FROM store_sales ss
LEFT JOIN products p ON ss.product_id = p.id
WHERE ss.revenue > 1300
GROUP BY CASE WHEN YEAR(p.launch_date) < 2024 THEN 'before_2024' WHEN YEAR(p.launch_date) = 2024 THEN '2024' ELSE 'after_2024' END;
