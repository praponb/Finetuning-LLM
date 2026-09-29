SELECT
    product_id,
    SUM(units) AS total_units,
    COUNT(*) AS cnt,
    SUM(SUM(units)) OVER (ORDER BY product_id) AS cumulative_units
FROM store_sales
GROUP BY product_id
ORDER BY 4 DESC
