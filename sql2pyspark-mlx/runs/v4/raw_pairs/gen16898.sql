WITH cleaned AS (
    SELECT
        *,
        LENGTH(city) AS city_length,
        COALESCE(city, 'N/A') AS resolved_city
    FROM suppliers
    WHERE city IS NOT NULL
)
SELECT *
FROM (
    SELECT
        country,
        city,
        rating
    FROM cleaned
) src
PIVOT (
    SUM(rating)
    FOR city IN ('New York' AS new_york, 'Bangkok' AS bangkok)
)
