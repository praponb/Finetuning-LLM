WITH filtered AS (
    SELECT
        *,
        TRIM(city) AS clean_city,
        COALESCE(city, 'NONE') AS city_filled
    FROM suppliers
    WHERE city IS NOT NULL
)
SELECT
    *,
    LAG(rating, 2) OVER (PARTITION BY country, city ORDER BY rating, id) AS prev_rating,
    LAST_VALUE(rating, TRUE) OVER w_running AS last_known_rating
FROM filtered
WINDOW w_running AS (PARTITION BY country ORDER BY rating, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW);
