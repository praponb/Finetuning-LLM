SELECT CASE WHEN YEAR(sale_date) < 2024 THEN 'before_2024' WHEN YEAR(sale_date) = 2024 THEN '2024' ELSE 'after_2024' END AS sale_cohort, COUNT(units) AS units_count, ROUND(AVG(units), 2) AS avg_units
FROM store_sales
GROUP BY 1
ORDER BY units_count DESC;
