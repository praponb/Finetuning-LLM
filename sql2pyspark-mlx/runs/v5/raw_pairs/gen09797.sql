SELECT
    DATE_TRUNC('MONTH', sale_date) AS sale_month,
    store_id,
    MAX(units) AS max_units,
    COUNT(*) AS cnt
FROM store_sales
GROUP BY 1, 2
HAVING COUNT(*) < 2
ORDER BY cnt
