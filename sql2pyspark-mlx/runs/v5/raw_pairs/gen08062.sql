SELECT
    id,
    brand,
    cost,
    category
FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY brand ORDER BY cost, id) rn
    FROM products
) t
WHERE rn <= 3
ORDER BY brand DESC, cost DESC;
