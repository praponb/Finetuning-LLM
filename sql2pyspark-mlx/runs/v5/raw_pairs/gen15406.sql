WITH numbered AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY country ORDER BY rating DESC, id) AS row_num
    FROM suppliers
    WHERE rating >= 2.1
)
SELECT
    id,
    country,
    rating,
    city
FROM numbered
WHERE row_num <= 5
