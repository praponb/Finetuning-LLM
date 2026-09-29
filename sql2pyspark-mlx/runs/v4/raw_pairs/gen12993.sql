SELECT
    product_id,
    CASE WHEN units >= 42 THEN 'high' WHEN units >= 6 THEN 'mid' ELSE 'low' END AS units_bucket,
    AVG(units) AS avg_units,
    COUNT(DISTINCT revenue) AS unique_products
FROM store_sales
WHERE product_id IN (4, 7, 9)
GROUP BY 1, 2
ORDER BY unique_products DESC;
