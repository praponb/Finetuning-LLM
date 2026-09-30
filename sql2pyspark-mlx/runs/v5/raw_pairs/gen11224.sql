SELECT p.category, COUNT(ss.id) AS store_sale_count, SUM(ss.revenue) AS total_revenue
FROM products p
LEFT JOIN store_sales ss ON p.id = ss.product_id
GROUP BY p.category
ORDER BY total_revenue DESC;
