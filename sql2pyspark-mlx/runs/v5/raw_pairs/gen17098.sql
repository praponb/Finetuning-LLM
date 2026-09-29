WITH numbered AS (
    SELECT *, DENSE_RANK() OVER (PARTITION BY region ORDER BY opened_date DESC) AS pos
    FROM stores
)
SELECT
    id,
    region,
    opened_date,
    store_name
FROM numbered
WHERE pos = 1
ORDER BY 3;
