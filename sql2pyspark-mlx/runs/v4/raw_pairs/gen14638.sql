SELECT id, brand, cost
FROM (
    SELECT
        id,
        brand,
        cost,
        ROW_NUMBER() OVER (PARTITION BY brand ORDER BY cost DESC, id) AS row_num
    FROM products
) AS ranked
WHERE row_num = 1
ORDER BY cost DESC;
