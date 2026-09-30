WITH base AS (
    SELECT *
    FROM store_sales
    WHERE product_id IN (8, 10)
),
agg AS (
    SELECT CASE WHEN YEAR(sale_date) < 2024 THEN 'before_2024' WHEN YEAR(sale_date) = 2024 THEN '2024' ELSE 'after_2024' END AS sale_cohort, MAX(sale_date) AS latest_sale_date, MAX(units) - MIN(units) AS units_range
    FROM base
    GROUP BY CASE WHEN YEAR(sale_date) < 2024 THEN 'before_2024' WHEN YEAR(sale_date) = 2024 THEN '2024' ELSE 'after_2024' END
)
SELECT *
FROM agg
WHERE units_range >= 64;
