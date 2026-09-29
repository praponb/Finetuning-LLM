SELECT
    id,
    category,
    cost,
    brand
FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY category ORDER BY cost, id) AS row_num
    FROM products
) AS r
WHERE row_num <= 3
ORDER BY cost DESC, brand DESC;
