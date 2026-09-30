WITH base AS (
    SELECT *
    FROM store_sales
    WHERE store_id = 2
),
summary AS (
    SELECT store_id, CASE WHEN YEAR(sale_date) < 2024 THEN 'before_2024' WHEN YEAR(sale_date) = 2024 THEN '2024' ELSE 'after_2024' END AS sale_cohort, AVG(units) AS avg_units
    FROM base
    GROUP BY store_id, CASE WHEN YEAR(sale_date) < 2024 THEN 'before_2024' WHEN YEAR(sale_date) = 2024 THEN '2024' ELSE 'after_2024' END
)
SELECT *
FROM summary
