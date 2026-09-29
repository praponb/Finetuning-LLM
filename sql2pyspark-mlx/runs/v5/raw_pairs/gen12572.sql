WITH numbered AS (
    SELECT
        id,
        region,
        opened_date,
        DENSE_RANK() OVER (PARTITION BY region ORDER BY opened_date DESC) AS pos
    FROM stores
    WHERE city IN ('London', 'Berlin', 'Bangkok')
)
SELECT *
FROM numbered
WHERE pos <= 3
ORDER BY opened_date DESC
