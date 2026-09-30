SELECT *
FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY city ORDER BY rating DESC, id ASC) AS rn
    FROM suppliers
) t
WHERE rn = 1;
