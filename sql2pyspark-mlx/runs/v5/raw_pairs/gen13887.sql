SELECT id, city, rating
FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY city ORDER BY rating DESC, id) AS rn
    FROM suppliers
) AS r
WHERE rn <= 2;
