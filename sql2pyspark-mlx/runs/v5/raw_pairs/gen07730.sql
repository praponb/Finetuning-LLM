WITH base AS (
    SELECT
        id,
        supplier_name,
        country,
        city,
        rating,
        ROUND(rating, 0) AS rating_rounded,
        CAST(rating AS INT) AS rating_int
    FROM suppliers
    WHERE rating IS NOT NULL
)
SELECT
    id,
    country,
    city,
    rating,
    rating_rounded,
    rating_int,
    ROW_NUMBER() OVER (PARTITION BY city ORDER BY rating_rounded, id) AS row_seq,
    ROUND(rating / SUM(rating) OVER (PARTITION BY country), 4) AS rating_share
FROM base;
