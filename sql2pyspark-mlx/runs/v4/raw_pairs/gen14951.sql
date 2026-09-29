WITH filtered_store_sales AS (
    SELECT *
    FROM store_sales
    WHERE product_id IN (4, 7, 8) AND store_id IN (2, 4, 5)
),
store_sales_stats AS (
    SELECT CASE WHEN YEAR(sale_date) < 2024 THEN 'before_2024' WHEN YEAR(sale_date) = 2024 THEN '2024' ELSE 'after_2024' END AS sale_cohort, COUNT(units) AS units_count, MAX(revenue) - MIN(revenue) AS revenue_range
    FROM filtered_store_sales
    GROUP BY CASE WHEN YEAR(sale_date) < 2024 THEN 'before_2024' WHEN YEAR(sale_date) = 2024 THEN '2024' ELSE 'after_2024' END
)
SELECT sale_cohort, units_count, revenue_range
FROM store_sales_stats
WHERE units_count > 9
ORDER BY 1 ASC
LIMIT 50
