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
        DENSE_RANK() OVER (PARTITION BY city ORDER BY rating DESC) row_num
    FROM suppliers
    WHERE city = 'Singapore'
) r
WHERE row_num = 1
