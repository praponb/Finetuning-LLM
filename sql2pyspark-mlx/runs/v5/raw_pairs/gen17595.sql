WITH recent_store_sales AS (
    SELECT *, CAST(revenue AS INT) AS revenue_int
    FROM store_sales
    WHERE revenue < 3100
),
Agg AS (
    SELECT product_id, MAX(revenue_int) AS max_revenue_int
    FROM recent_store_sales
    GROUP BY product_id
)
SELECT product_id, max_revenue_int
FROM Agg
WHERE max_revenue_int < 29
ORDER BY product_id DESC
