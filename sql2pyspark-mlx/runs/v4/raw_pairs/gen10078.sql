WITH src AS (
    SELECT *, units - revenue revenue_per_unit
    FROM store_sales
    WHERE product_id IN (1, 4, 10) AND product_id = 5
),
store_sales_stats AS (
    SELECT store_id, product_id, AVG(revenue) avg_revenue
    FROM src
    GROUP BY store_id, product_id
)
SELECT *
FROM store_sales_stats
ORDER BY store_id DESC, product_id
LIMIT 10
