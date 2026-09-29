SELECT
    CASE WHEN YEAR(sale_date) < 2024 THEN 'before_2024' WHEN YEAR(sale_date) = 2024 THEN '2024' ELSE 'after_2024' END AS sale_cohort,
    MAX(revenue) - MIN(revenue) AS revenue_range,
    MIN(sale_date) AS min_sale_date,
    AVG(revenue) AS avg_revenue
FROM store_sales
GROUP BY sale_cohort
