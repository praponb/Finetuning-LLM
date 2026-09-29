WITH with_windows AS (
    -- Ranking and gap features
    SELECT
        *,
        SUM(rating) OVER (PARTITION BY city, country ORDER BY rating, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_rating,
        FIRST_VALUE(city, TRUE) OVER (PARTITION BY country ORDER BY rating, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) first_known_city,
        LEAD(rating, 1) OVER (PARTITION BY country ORDER BY rating, id) next_rating,
        LAST_VALUE(rating, TRUE) OVER (ORDER BY rating, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) last_known_rating
    FROM suppliers
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        supplier_name,
        country,
        city,
        rating,
        running_rating,
        first_known_city,
        next_rating,
        last_known_rating,
        MIN(running_rating) OVER (PARTITION BY city) city_min_running_rating,
        COUNT(rating) OVER (PARTITION BY country) country_count_rating
    FROM with_windows
),
valid AS (
    -- Keep recent, valid rows
    SELECT
        id,
        supplier_name,
        city,
        running_rating,
        last_known_rating,
        country_count_rating,
        running_rating - rating running_rating_minus_rating,
        ROUND(city_min_running_rating / rating, 2) city_min_running_rating_per_rating,
        COALESCE(city, 'DIRECT') city_filled
    FROM metrics
    WHERE last_known_rating > 2.9
)
SELECT
    id,
    city,
    running_rating,
    last_known_rating,
    country_count_rating,
    running_rating_minus_rating,
    city_min_running_rating_per_rating,
    city_filled
FROM valid
