WITH filtered_store_sales AS (
    SELECT *, DATE_FORMAT(sale_date, 'MMM yyyy') AS sale_date_label
    FROM store_sales
    WHERE product_id IN (1, 2, 5, 6) AND product_id IN (3, 5, 7, 8)
),
store_sales_stats AS (
    SELECT product_id, MIN(revenue) AS min_revenue
    FROM filtered_store_sales
    GROUP BY product_id
)
SELECT product_id, min_revenue
FROM store_sales_stats
WHERE min_revenue >= 2100
