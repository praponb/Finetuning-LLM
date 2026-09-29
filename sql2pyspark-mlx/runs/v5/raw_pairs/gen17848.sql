SELECT id, brand, price
FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY brand ORDER BY price DESC, id) AS pos
    FROM products
    WHERE price IS NOT NULL
) AS t
WHERE pos <= 2
ORDER BY brand DESC;
