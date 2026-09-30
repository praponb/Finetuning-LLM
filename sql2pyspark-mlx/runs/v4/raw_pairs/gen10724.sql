SELECT
    product_id,
    QUARTER(sale_date) AS sale_quarter,
    store_id,
    ROUND(AVG(units), 2) AS avg_units,
    COUNT(*) AS total_count
FROM store_sales
WHERE revenue > 3300
GROUP BY product_id, sale_quarter, store_id
HAVING COUNT(*) < 4
ORDER BY avg_units;
