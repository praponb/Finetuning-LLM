WITH cleaned AS (
    -- Keep recent, valid rows
    SELECT
        *,
        LOWER(country) AS country_lower,
        COALESCE(city, 'N/A') AS city_clean
    FROM suppliers
    WHERE city IS NOT DISTINCT FROM 'Tokyo'
      AND rating IS NOT NULL
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        id,
        country,
        city,
        country_lower,
        city_clean,
        LEAD(rating, 2) OVER (ORDER BY rating ASC, id ASC) AS next_rating,
        NTILE(4) OVER (PARTITION BY city ORDER BY rating ASC, id ASC) AS rating_bucket,
        LEAD(rating, 1) OVER (PARTITION BY city ORDER BY rating ASC, id ASC) AS next_rating_2,
        COUNT(rating) OVER (PARTITION BY country) AS country_count_rating,
        SUM(rating) OVER (PARTITION BY country, city) AS country_sum_rating
    FROM cleaned
),
with_last AS (
    -- Latest matching product per row (LATERAL ... LIMIT 1)
    SELECT s.*, top1.product_launch_date
    FROM with_windows s
    LEFT JOIN LATERAL (
            SELECT c.launch_date AS product_launch_date
            FROM products c
            WHERE c.supplier_id = s.id
            ORDER BY c.launch_date DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
ranked AS (
    -- QUALIFY-style window filter
    SELECT *, ROW_NUMBER() OVER (PARTITION BY country ORDER BY next_rating DESC NULLS LAST, id ASC) AS rn
    FROM with_last
    QUALIFY rn = 1
)
SELECT
    id,
    country,
    city,
    city_clean,
    next_rating,
    rating_bucket,
    next_rating_2,
    country_count_rating,
    product_launch_date
FROM ranked
ORDER BY product_launch_date DESC;
