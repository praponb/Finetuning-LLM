SELECT CASE WHEN YEAR(opened_date) < 2024 THEN 'before_2024' WHEN YEAR(opened_date) = 2024 THEN '2024' ELSE 'after_2024' END opened_cohort, COUNT(DISTINCT sqft) unique_citys
FROM stores
GROUP BY 1
ORDER BY unique_citys, opened_cohort
LIMIT 10;
