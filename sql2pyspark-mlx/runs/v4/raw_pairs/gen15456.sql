SELECT *
FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY city ORDER BY opened_date, id) AS pos
    FROM stores
    WHERE region IN ('West', 'East')
) AS t
WHERE pos <= 3
ORDER BY opened_date DESC, pos;
