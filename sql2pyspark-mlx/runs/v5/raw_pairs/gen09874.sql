WITH product_summary AS (
    SELECT product_id, MAX(sale_date) AS last_sale_date, COUNT(revenue) AS revenue_count
    FROM store_sales
    WHERE product_id IN (3, 4, 6)
    GROUP BY product_id
)
SELECT
    p.category,
    p.product_name,
    ps.last_sale_date,
    ps.revenue_count
FROM products p
LEFT JOIN product_summary ps ON p.id = ps.product_id
WHERE ps.revenue_count >= 7
ORDER BY p.product_name DESC;
