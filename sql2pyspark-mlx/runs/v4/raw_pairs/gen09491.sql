SELECT
    CASE WHEN YEAR(sale_date) < 2024 THEN 'before_2024' WHEN YEAR(sale_date) = 2024 THEN '2024' ELSE 'after_2024' END sale_cohort,
    SUM(units) total_units,
    COUNT(*) cnt,
    RANK() OVER (ORDER BY SUM(units) DESC) AS units_rank
FROM store_sales
GROUP BY CASE WHEN YEAR(sale_date) < 2024 THEN 'before_2024' WHEN YEAR(sale_date) = 2024 THEN '2024' ELSE 'after_2024' END
ORDER BY sale_cohort;
