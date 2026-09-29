SELECT
    p.id,
    p.product_name,
    COUNT(ss.id) AS store_sale_count,
    SUM(ss.revenue) AS total_revenue,
    MIN(ss.sale_date) AS min_sale_date
FROM products p
INNER JOIN store_sales ss ON p.id = ss.product_id
GROUP BY p.id, p.product_name;
