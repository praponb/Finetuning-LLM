SELECT
    CASE WHEN YEAR(sale_date) < 2024 THEN 'before_2024' WHEN YEAR(sale_date) = 2024 THEN '2024' ELSE 'after_2024' END AS sale_cohort,
    COUNT(revenue) AS revenue_count,
    ROUND(AVG(units), 2) AS avg_units,
    MAX(sale_date) AS last_sale_date
FROM store_sales
WHERE store_id IN (2, 4, 5)
GROUP BY CASE WHEN YEAR(sale_date) < 2024 THEN 'before_2024' WHEN YEAR(sale_date) = 2024 THEN '2024' ELSE 'after_2024' END
HAVING COUNT(revenue) >= 3
ORDER BY last_sale_date DESC, avg_units DESC, sale_cohort ASC
LIMIT 1
