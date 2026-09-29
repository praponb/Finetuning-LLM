WITH filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        supplier_name,
        country,
        city,
        rating,
        TRIM(country) AS clean_country,
        CONCAT(supplier_name, ', ', country) AS supplier_name_country,
        COALESCE(city, 'N/A') AS city_filled
    FROM suppliers
    WHERE city IS DISTINCT FROM 'Singapore'
),
valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, FLOOR(rating) AS rating_whole
    FROM filtered
    WHERE city IS NOT NULL
),
cleaned AS (
    -- Keep recent, valid rows
    SELECT *, CONCAT(country, ', ', city) AS country_city
    FROM valid
    WHERE city IS NOT NULL
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        supplier_name,
        city,
        rating,
        city_filled,
        rating_whole,
        AVG(rating_whole) OVER (PARTITION BY country) AS country_avg_rating_whole,
        SUM(rating_whole) OVER (PARTITION BY country, city ORDER BY rating_whole, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_rating_whole,
        LAST_VALUE(rating) IGNORE NULLS OVER w_running AS last_known_rating
    FROM cleaned
    WHERE country IN ('JP', 'US', 'UK')
    WINDOW w_running AS (PARTITION BY city ORDER BY rating_whole, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
filtered_suppliers AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        city,
        rating,
        city_filled,
        rating_whole,
        country_avg_rating_whole,
        cumulative_rating_whole,
        last_known_rating,
        GREATEST(rating, last_known_rating) AS greatest_rating_last_known_rating,
        COALESCE(city, 'NONE') AS city_filled_2
    FROM with_windows
    WHERE rating_whole IS NULL
),
kept AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM filtered_suppliers s
    WHERE NOT EXISTS (
            SELECT 1
            FROM products ch
            WHERE ch.supplier_id = s.id
              AND ch.price BETWEEN 560 AND 720
        )
)
SELECT
    id,
    rating,
    city_filled,
    rating_whole,
    country_avg_rating_whole,
    cumulative_rating_whole,
    greatest_rating_last_known_rating,
    city_filled_2
FROM kept
ORDER BY rating NULLS FIRST
