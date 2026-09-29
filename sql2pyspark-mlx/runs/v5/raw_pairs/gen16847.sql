SELECT CASE WHEN YEAR(launch_date) < 2024 THEN 'before_2024' WHEN YEAR(launch_date) = 2024 THEN '2024' ELSE 'after_2024' END launch_cohort, brand, SUM(CASE WHEN brand = 'Initech' THEN stock_qty ELSE 0 END) initech_stock_qty
FROM products
WHERE launch_date > '2025-02-01'
GROUP BY CASE WHEN YEAR(launch_date) < 2024 THEN 'before_2024' WHEN YEAR(launch_date) = 2024 THEN '2024' ELSE 'after_2024' END, brand
ORDER BY launch_cohort DESC, brand
LIMIT 3
