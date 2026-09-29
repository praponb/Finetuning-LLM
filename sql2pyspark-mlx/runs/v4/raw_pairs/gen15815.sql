WITH filtered_store_sales AS (
    SELECT *, ROUND(revenue, 1) AS revenue_rounded
    FROM store_sales
    WHERE sale_date BETWEEN '2024-05-01' AND '2024-08-01' AND units <= 34 AND units > 13
),
store_sales_stats AS (
    SELECT product_id, QUARTER(sale_date) AS sale_quarter, ROUND(STDDEV(revenue), 2) AS stddev_revenue
    FROM filtered_store_sales
    GROUP BY product_id, QUARTER(sale_date)
)
SELECT product_id, sale_quarter, stddev_revenue
FROM store_sales_stats
ORDER BY product_id;
