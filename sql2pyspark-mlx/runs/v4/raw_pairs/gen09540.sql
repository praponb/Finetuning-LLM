WITH numbered AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY status ORDER BY discount, id) AS rnk
    FROM orders
)
SELECT
    id,
    status,
    discount,
    channel
FROM numbered
WHERE rnk <= 2
