SELECT
    DATE_FORMAT(sale_date, 'yyyy-MM') AS sale_month,
    store_id,
    MIN(sale_date) AS min_sale_date,
    MAX(revenue) AS max_revenue
FROM store_sales
GROUP BY 1, 2
HAVING AVG(revenue) < 2500
ORDER BY sale_month DESC
