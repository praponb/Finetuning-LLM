WITH base AS (
    SELECT *, TRIM(country) AS clean_country
    FROM suppliers
    WHERE rating IS NOT NULL
),
suppliers_stats AS (
    SELECT country, SORT_ARRAY(COLLECT_SET(city)) AS city_list, AVG(rating) AS avg_rating
    FROM base
    GROUP BY country
)
SELECT country, city_list, avg_rating
FROM suppliers_stats
WHERE avg_rating > 2.2
ORDER BY country
LIMIT 50;
