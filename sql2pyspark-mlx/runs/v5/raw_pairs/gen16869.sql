WITH valid_store_sales AS (
    SELECT *, DATE_TRUNC('month', sale_date) AS sale_date_period
    FROM store_sales
    WHERE revenue < 3900 OR units IS NOT NULL
),
store_sales_stats AS (
    SELECT CASE WHEN YEAR(sale_date_period) < 2024 THEN 'before_2024' WHEN YEAR(sale_date_period) = 2024 THEN '2024' ELSE 'after_2024' END AS sale_period_cohort, MAX(units) AS max_units
    FROM valid_store_sales
    GROUP BY CASE WHEN YEAR(sale_date_period) < 2024 THEN 'before_2024' WHEN YEAR(sale_date_period) = 2024 THEN '2024' ELSE 'after_2024' END
)
SELECT *
FROM store_sales_stats
WHERE max_units > 28
ORDER BY sale_period_cohort DESC
