WITH numbered AS (
    SELECT
        id,
        status,
        amount,
        ROW_NUMBER() OVER (PARTITION BY status ORDER BY amount DESC, id) AS rnk
    FROM orders
)
SELECT *
FROM numbered
WHERE rnk <= 3;
