SELECT *
FROM (
    SELECT
        id,
        country,
        rating,
        supplier_name,
        DENSE_RANK() OVER (PARTITION BY country ORDER BY rating DESC) AS pos
    FROM suppliers
    WHERE city = 'Tokyo'
) AS r
WHERE pos <= 1
ORDER BY rating ASC;
