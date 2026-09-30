SELECT *
FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY city ORDER BY opened_date DESC, id) AS rn
    FROM stores
) AS ranked
WHERE rn <= 3;
