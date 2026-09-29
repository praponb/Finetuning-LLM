SELECT
    id,
    city,
    rating,
    supplier_name
FROM (
    SELECT
        id,
        city,
        rating,
        supplier_name,
        ROW_NUMBER() OVER (PARTITION BY city ORDER BY rating, id) AS pos
    FROM suppliers
) AS t
WHERE pos <= 3
ORDER BY rating DESC
