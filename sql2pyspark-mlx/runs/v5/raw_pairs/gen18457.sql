WITH src AS (
    SELECT *
    FROM products
    WHERE cost >= 570 OR stock_qty = 243
),
agg AS (
    SELECT
        CASE WHEN YEAR(launch_date) < 2024 THEN 'before_2024' WHEN YEAR(launch_date) = 2024 THEN '2024' ELSE 'after_2024' END launch_cohort,
        brand,
        SUM(CASE WHEN brand = 'Acme' THEN cost ELSE 0 END) acme_cost,
        ROUND((100 * SUM(CASE WHEN brand = 'Acme' THEN 1 ELSE 0 END)) / COUNT(*), 1) pct_acme
    FROM src
    GROUP BY CASE WHEN YEAR(launch_date) < 2024 THEN 'before_2024' WHEN YEAR(launch_date) = 2024 THEN '2024' ELSE 'after_2024' END, brand
)
SELECT *
FROM agg
