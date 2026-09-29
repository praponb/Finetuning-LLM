WITH cleaned AS (
    SELECT *, UPPER(country) AS country_upper
    FROM suppliers
    WHERE city = 'Berlin'
),
cleaned_suppliers AS (
    SELECT
        id,
        city,
        rating,
        country_upper,
        COALESCE(rating, 0) AS rating_filled,
        CAST(rating AS INT) AS rating_int,
        COALESCE(city, 'N/A') AS resolved_city
    FROM cleaned
    WHERE city IN ('Tokyo', 'Berlin')
),
enriched AS (
    SELECT
        id,
        city,
        rating,
        country_upper,
        rating_filled,
        rating_int,
        resolved_city,
        NTILE(5) OVER w_ordered AS rating_bucket,
        NTILE(4) OVER (ORDER BY rating_filled DESC, id) AS rating_filled_bucket
    FROM cleaned_suppliers
    WINDOW w_ordered AS (PARTITION BY city ORDER BY rating, id)
)
SELECT
    id,
    city,
    rating,
    country_upper,
    rating_filled,
    rating_int,
    resolved_city,
    rating_filled_bucket
FROM enriched
ORDER BY rating_int DESC;
