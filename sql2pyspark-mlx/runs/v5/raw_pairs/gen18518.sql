WITH base AS (
    SELECT
        id,
        country,
        city,
        rating,
        COALESCE(rating, 0) AS rating_filled,
        UPPER(country) AS country_upper,
        COALESCE(city, 'N/A') AS city_filled
    FROM suppliers
    WHERE city IS NOT NULL
),
scored AS (
    SELECT
        id,
        rating,
        rating_filled,
        country_upper,
        LAG(rating_filled) OVER (PARTITION BY country, city ORDER BY rating_filled, id) AS prev_rating_filled,
        LAG(rating) OVER (PARTITION BY country, city ORDER BY rating_filled, id) AS prev_rating,
        ROW_NUMBER() OVER (PARTITION BY country, city ORDER BY rating_filled, id) AS row_seq,
        SUM(rating) OVER (PARTITION BY country ORDER BY rating, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_rating
    FROM base
    WHERE city = 'Singapore'
),
latest_match AS (
    SELECT s.*, top1.brand
    FROM scored s
    JOIN LATERAL (
            SELECT c.brand
            FROM products c
            WHERE c.supplier_id = s.id
            ORDER BY c.price DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
with_windows AS (
    SELECT
        *,
        SUM(prev_rating) OVER w_grp AS running_sum_prev_rating,
        MIN(prev_rating) OVER w_grp AS running_min_prev_rating,
        COUNT(prev_rating) OVER w_grp AS running_count_prev_rating
    FROM latest_match
    WINDOW w_grp AS (PARTITION BY brand ORDER BY prev_rating, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
)
SELECT
    id,
    rating,
    prev_rating,
    row_seq,
    running_rating,
    brand,
    running_sum_prev_rating,
    running_min_prev_rating,
    running_count_prev_rating,
    CASE WHEN brand RLIKE '^(Umbrella|Initech)$' THEN 'OTHER_ZONE' ELSE 'GROUP_B' END AS brand_zone
FROM with_windows;
