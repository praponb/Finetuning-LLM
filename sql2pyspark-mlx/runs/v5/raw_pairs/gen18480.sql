WITH Numbered AS (
    SELECT *, RANK() OVER (PARTITION BY store_id ORDER BY quantity DESC) AS row_num
    FROM inventory
    WHERE quantity <= 162
)
SELECT
    id,
    store_id,
    quantity,
    product_id
FROM Numbered
WHERE row_num <= 1
ORDER BY quantity DESC;
