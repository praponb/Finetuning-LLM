WITH product_stats AS (
    SELECT product_id, MIN(sale_date) AS min_sale_date, AVG(revenue) AS avg_revenue
    FROM store_sales
    GROUP BY product_id
)
SELECT
    p.id,
    p.category,
    p.brand,
    ps.min_sale_date,
    ps.avg_revenue
FROM products p
LEFT OUTER JOIN product_stats ps ON p.id = ps.product_id
WHERE ps.avg_revenue >= 4000
ORDER BY p.id;
