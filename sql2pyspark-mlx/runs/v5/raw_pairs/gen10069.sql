SELECT *
FROM (
    SELECT *, RANK() OVER (PARTITION BY city ORDER BY opened_date DESC) AS pos
    FROM stores
) AS t
WHERE pos = 1
ORDER BY 3, opened_date
