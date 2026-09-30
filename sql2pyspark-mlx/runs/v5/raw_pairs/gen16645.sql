WITH Numbered AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY city ORDER BY sqft DESC, id) AS pos
    FROM stores
)
SELECT id, city, sqft
FROM Numbered
WHERE pos = 1
ORDER BY id ASC, city ASC;
