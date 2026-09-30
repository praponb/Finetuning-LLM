WITH numbered AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY city ORDER BY rating DESC, id) rnk
    FROM suppliers
)
SELECT id, city, rating
FROM numbered
WHERE rnk <= 3
ORDER BY 1
