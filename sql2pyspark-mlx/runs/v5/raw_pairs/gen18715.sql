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
        ROW_NUMBER() OVER (PARTITION BY country ORDER BY rating, id) AS rn
    FROM suppliers
) AS ranked
WHERE rn <= 3;
