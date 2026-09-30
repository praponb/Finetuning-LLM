SELECT
    CASE WHEN revenue >= 4200 THEN 'high' WHEN revenue >= 1800 THEN 'mid' ELSE 'low' END AS revenue_bucket,
    DATE_TRUNC('MONTH', sale_date) AS sale_month,
    MIN(sale_date) AS first_sale_date,
    COUNT(DISTINCT revenue) AS unique_products
FROM store_sales
WHERE store_id IN (1, 2, 3, 4)
GROUP BY 1, 2
ORDER BY first_sale_date ASC;
