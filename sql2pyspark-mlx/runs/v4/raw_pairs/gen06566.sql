SELECT
    id,
    store_id,
    revenue,
    units
FROM (
    SELECT *, RANK() OVER (PARTITION BY store_id ORDER BY revenue) AS pos
    FROM store_sales
) AS ranked
WHERE pos = 1
ORDER BY revenue DESC;
