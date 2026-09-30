WITH ranked AS (
    SELECT
        id,
        store_id,
        units,
        revenue,
        ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY units DESC, id) AS pos
    FROM store_sales
    WHERE revenue BETWEEN 2200 AND 3200
)
SELECT
    id,
    store_id,
    units,
    revenue
FROM ranked
WHERE pos <= 1
