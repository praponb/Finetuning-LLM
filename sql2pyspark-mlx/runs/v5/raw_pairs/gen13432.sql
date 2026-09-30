SELECT
    YEAR(ss.sale_date) AS sale_year,
    CASE WHEN YEAR(ss.sale_date) < 2024 THEN 'before_2024' WHEN YEAR(ss.sale_date) = 2024 THEN '2024' ELSE 'after_2024' END AS sale_cohort,
    MIN(i.quantity) AS min_quantity,
    COUNT(DISTINCT i.quantity) AS unique_stores
FROM inventory i
LEFT JOIN store_sales ss ON i.store_id = ss.store_id AND i.product_id = ss.product_id
GROUP BY YEAR(ss.sale_date), CASE WHEN YEAR(ss.sale_date) < 2024 THEN 'before_2024' WHEN YEAR(ss.sale_date) = 2024 THEN '2024' ELSE 'after_2024' END
ORDER BY min_quantity DESC, sale_year, sale_cohort
LIMIT 1;
