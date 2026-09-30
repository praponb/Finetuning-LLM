WITH numbered AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY city ORDER BY rating DESC, id) AS row_num
    FROM suppliers
)
SELECT
    id,
    city,
    rating,
    country
FROM numbered
WHERE row_num <= 3
