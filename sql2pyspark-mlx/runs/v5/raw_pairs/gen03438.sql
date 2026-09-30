WITH cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        COALESCE(rating, 0) rating_filled,
        COALESCE(city, 'OTHER') resolved_city
    FROM suppliers
    WHERE rating IS NOT NULL
)
SELECT country
FROM cleaned
INTERSECT
SELECT country
FROM suppliers
WHERE city IN ('Berlin', 'Singapore')
