SELECT sale_year, store_sale_count, unique_stores
FROM (
    SELECT YEAR(sale_date) AS sale_year, COUNT(*) AS store_sale_count, COUNT(DISTINCT revenue) AS unique_stores
    FROM store_sales
    WHERE store_id IN (3, 4, 5, 6)
    GROUP BY YEAR(sale_date)
) AS x
WHERE unique_stores < 3
ORDER BY store_sale_count DESC;
