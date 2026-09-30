SELECT *
FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY city ORDER BY age DESC, id) AS rn
    FROM customers
) AS r
WHERE rn = 1
ORDER BY name ASC
