SELECT
    id,
    country,
    rating,
    supplier_name
FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY country ORDER BY rating DESC, id) AS row_num
    FROM suppliers
    WHERE rating >= 2.6
) AS t
WHERE row_num <= 2;
