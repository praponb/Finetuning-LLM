WITH cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        supplier_name,
        country,
        city,
        rating,
        LENGTH(country) AS country_length
    FROM suppliers
    WHERE city IS NOT NULL
),
base AS (
    -- Keep recent, valid rows
    SELECT
        *,
        ROUND(country_length, 2) AS country_length_rounded,
        COALESCE(city, 'DIRECT') AS city_filled
    FROM cleaned
    WHERE city = 'Chiang Mai'
),
with_last AS (
    -- Latest matching product per row (LATERAL ... LIMIT 1)
    SELECT s.*, latest.product_category
    FROM base s
    JOIN LATERAL (
            SELECT c.category AS product_category
            FROM products c
            WHERE c.supplier_id = s.id
            ORDER BY c.price DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
scored AS (
    -- Running and moving aggregates
    SELECT
        *,
        LAG(country_length_rounded, 1) OVER w_part AS prev_country_length_rounded,
        AVG(rating) OVER w_running AS rating_avg_2_prior,
        SUM(country_length_rounded) OVER (PARTITION BY product_category ORDER BY rating, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_country_length_rounded,
        LEAD(country_length_rounded, 1) OVER (PARTITION BY country, city ORDER BY rating, id) AS next_country_length_rounded,
        AVG(country_length_rounded) OVER (PARTITION BY city ORDER BY country_length_rounded, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS country_length_rounded_avg_6
    FROM with_last
    WINDOW w_part AS (PARTITION BY country ORDER BY country_length, id), w_running AS (PARTITION BY country, product_category ORDER BY country_length_rounded, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING)
)
-- Native SQL PIVOT
SELECT *
FROM (
    SELECT
        product_category,
        city,
        running_country_length_rounded
    FROM scored
) src
PIVOT (
    AVG(running_country_length_rounded) AS total
    FOR city IN ('Berlin', 'Tokyo')
);
