SELECT *
FROM (
    SELECT
        id,
        country,
        rating,
        supplier_name,
        ROW_NUMBER() OVER (PARTITION BY country ORDER BY rating, id) AS row_num
    FROM suppliers
    WHERE country IN ('DE', 'TH', 'JP')
) AS ranked
WHERE row_num = 1;
