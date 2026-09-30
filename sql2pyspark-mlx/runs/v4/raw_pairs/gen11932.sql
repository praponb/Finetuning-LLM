SELECT
    DATE_TRUNC('MONTH', sale_date) AS sale_month,
    store_id,
    AVG(units) AS avg_units,
    COUNT(DISTINCT units) AS unique_stores
FROM store_sales
WHERE MONTH(sale_date) IN (2, 4, 8)
GROUP BY DATE_TRUNC('MONTH', sale_date), store_id
ORDER BY unique_stores DESC;
