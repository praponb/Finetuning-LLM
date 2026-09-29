WITH Ranked AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY country ORDER BY age DESC, id) AS pos
    FROM customers
)
SELECT *
FROM Ranked
WHERE pos <= 5;
