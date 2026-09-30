WITH Ranked AS (
    SELECT *, DENSE_RANK() OVER (PARTITION BY product_id ORDER BY quantity) AS rn
    FROM inventory
)
SELECT
    id,
    product_id,
    quantity,
    store_id
FROM Ranked
WHERE rn = 1;
