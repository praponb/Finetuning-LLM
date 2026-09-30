WITH base AS (
    SELECT *
    FROM store_sales
    WHERE sale_date < '2024-02-01' AND sale_date >= '2025-02-19'
),
store_sales_stats AS (
    SELECT CASE WHEN revenue >= 3000 THEN 'high' WHEN revenue >= 900 THEN 'mid' ELSE 'low' END AS revenue_bucket, MAX(sale_date) AS last_sale_date
    FROM base
    GROUP BY CASE WHEN revenue >= 3000 THEN 'high' WHEN revenue >= 900 THEN 'mid' ELSE 'low' END
)
SELECT *
FROM store_sales_stats
