SELECT
    id,
    store_id,
    revenue,
    units
FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY revenue DESC, id) AS row_num
    FROM store_sales
) AS t
WHERE row_num = 1
