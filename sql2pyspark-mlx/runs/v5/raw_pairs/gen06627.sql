SELECT id, city, rating
FROM (
    SELECT
        id,
        city,
        rating,
        ROW_NUMBER() OVER (PARTITION BY city ORDER BY rating DESC, id) row_num
    FROM suppliers
) t
WHERE row_num <= 2;
