WITH numbered AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY discount, id) AS rnk
    FROM orders
)
SELECT *
FROM numbered
WHERE rnk <= 3
ORDER BY status DESC
