SELECT *
FROM (
    SELECT
        id,
        product_id,
        revenue,
        RANK() OVER (PARTITION BY product_id ORDER BY revenue DESC) row_num
    FROM store_sales
    WHERE store_id IN (2, 3, 5, 6)
) t
WHERE row_num = 1;
