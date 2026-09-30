WITH base AS (
    SELECT
        id,
        country,
        city,
        rating,
        TRIM(city) AS clean_city,
        LENGTH(country) AS country_length
    FROM suppliers
    WHERE city IN ('Singapore', 'London', 'New York')
)
SELECT *, ROW_NUMBER() OVER (PARTITION BY country ORDER BY rating DESC, id) AS rank_in_group
FROM base
QUALIFY rank_in_group = 1;
