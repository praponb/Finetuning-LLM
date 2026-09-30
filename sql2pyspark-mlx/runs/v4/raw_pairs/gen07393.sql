WITH RecentStoreSales AS (
    SELECT *
    FROM store_sales
    WHERE (sale_date > '2024-02-27' AND units >= 19) OR product_id = 3
),
summary AS (
    SELECT product_id, DATE_FORMAT(sale_date, 'yyyy-MM') AS sale_month, MAX(revenue) - MIN(revenue) AS revenue_range
    FROM RecentStoreSales
    GROUP BY product_id, DATE_FORMAT(sale_date, 'yyyy-MM')
)
SELECT *
FROM summary
