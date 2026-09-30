SELECT
    product_id,
    DATE_FORMAT(sale_date, 'yyyy-MM') AS sale_month,
    MIN(sale_date) AS first_sale_date,
    COUNT(DISTINCT units) AS unique_products
FROM store_sales
GROUP BY product_id, DATE_FORMAT(sale_date, 'yyyy-MM')
ORDER BY 1 DESC, sale_month
LIMIT 5
