SELECT *
FROM (
    SELECT store_id, COUNT(DISTINCT units) AS unique_stores, COUNT(*) AS store_sale_count
    FROM store_sales
    GROUP BY store_id
) AS agg
WHERE store_sale_count >= 7
