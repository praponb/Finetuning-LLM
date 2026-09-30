SELECT id, store_id, revenue
FROM (
    SELECT
        id,
        store_id,
        revenue,
        RANK() OVER (PARTITION BY store_id ORDER BY revenue) row_num
    FROM store_sales
) r
WHERE row_num <= 5;
