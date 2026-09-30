SELECT
    id,
    city,
    rating,
    supplier_name
FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY city ORDER BY rating DESC, id) AS row_num
    FROM suppliers
    WHERE city IN ('Chiang Mai', 'London')
) AS t
WHERE row_num <= 5;
