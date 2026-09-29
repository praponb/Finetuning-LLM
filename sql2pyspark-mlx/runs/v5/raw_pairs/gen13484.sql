SELECT
    id,
    country,
    rating,
    supplier_name
FROM (
    SELECT
        id,
        country,
        rating,
        supplier_name,
        ROW_NUMBER() OVER (PARTITION BY country ORDER BY rating, id) AS row_num
    FROM suppliers
    WHERE rating BETWEEN 1.9 AND 3.3
) AS ranked
WHERE row_num <= 3;
