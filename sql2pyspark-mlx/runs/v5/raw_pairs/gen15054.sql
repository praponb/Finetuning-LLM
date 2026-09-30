WITH numbered AS (
    SELECT
        id,
        method,
        amount,
        ROW_NUMBER() OVER (PARTITION BY method ORDER BY amount DESC, id) AS row_num
    FROM payments
)
SELECT *
FROM numbered
WHERE row_num <= 3
ORDER BY row_num DESC
