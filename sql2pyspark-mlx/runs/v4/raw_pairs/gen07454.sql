WITH valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        TRIM(country) AS clean_country,
        ROUND(rating * 0.9, 2) AS rating_scaled
    FROM suppliers
    WHERE city IS NOT NULL
),
latest AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM valid
    QUALIFY ROW_NUMBER() OVER (PARTITION BY country ORDER BY rating_scaled DESC, id) = 1
),
base AS (
    -- Keep recent, valid rows
    SELECT
        *,
        CASE WHEN city = 'Chiang Mai' THEN 1 ELSE 0 END AS is_chiang_mai,
        GREATEST(rating, rating_scaled) AS greatest_rating_rating_scaled,
        COALESCE(city, 'DIRECT') AS city_clean
    FROM latest
    WHERE city IS NOT NULL
      AND rating_scaled < 44
),
with_last AS (
    -- Latest matching product per row (LATERAL ... LIMIT 1)
    SELECT s.*, top1.price
    FROM base s
    LEFT JOIN LATERAL (
            SELECT c.price
            FROM products c
            WHERE c.supplier_id = s.id
            ORDER BY c.price DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
)
SELECT
    id,
    country,
    city,
    clean_country,
    rating_scaled,
    is_chiang_mai,
    greatest_rating_rating_scaled,
    city_clean,
    price,
    CASE WHEN city RLIKE '^(Tokyo|Singapore)$' THEN 'PRIORITY' ELSE 'EXTENDED' END AS city_zone
FROM with_last
ORDER BY price DESC NULLS LAST
