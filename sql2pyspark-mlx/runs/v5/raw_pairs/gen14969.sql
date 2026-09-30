SELECT CASE WHEN YEAR(launch_date) < 2024 THEN 'before_2024' WHEN YEAR(launch_date) = 2024 THEN '2024' ELSE 'after_2024' END AS launch_cohort, brand, COUNT(DISTINCT price) AS unique_suppliers
FROM products
GROUP BY launch_cohort, brand
ORDER BY launch_cohort
