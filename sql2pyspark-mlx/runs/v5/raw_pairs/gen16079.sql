WITH numbered AS (
    SELECT *, RANK() OVER (PARTITION BY product_id ORDER BY quantity DESC) AS row_num
    FROM inventory
    WHERE product_id IN (2, 9, 10)
)
SELECT
    id,
    product_id,
    quantity,
    store_id
FROM numbered
WHERE row_num <= 3;
