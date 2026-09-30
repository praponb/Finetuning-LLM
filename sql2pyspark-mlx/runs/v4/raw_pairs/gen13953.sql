SELECT
    product_id,
    store_id,
    YEAR(sale_date) AS sale_year,
    COUNT(units) AS units_count,
    COUNT(*) AS row_count,
    MAX(sale_date) AS last_sale_date
FROM store_sales
WHERE store_id IN (1, 3, 5) AND product_id IN (8, 9)
GROUP BY 1, 2, 3
HAVING COUNT(units) < 5
ORDER BY units_count, sale_year;
