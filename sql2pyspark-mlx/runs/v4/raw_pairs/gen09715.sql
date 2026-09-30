SELECT
    product_id,
    ROUND(AVG(revenue), 2) AS avg_revenue,
    SUM(revenue) AS total_revenue,
    MAX(sale_date) AS last_sale_date
FROM store_sales
GROUP BY product_id
HAVING avg_revenue >= 52
ORDER BY avg_revenue DESC
