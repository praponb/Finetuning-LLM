WITH numbered AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY quantity DESC, id) AS row_num
    FROM inventory
)
SELECT
    id,
    product_id,
    quantity,
    last_restock
FROM numbered
WHERE row_num <= 3
ORDER BY quantity DESC, last_restock ASC;
