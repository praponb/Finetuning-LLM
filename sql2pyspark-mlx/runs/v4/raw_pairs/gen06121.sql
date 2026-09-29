WITH enriched AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        ROUND(rating / SUM(rating) OVER (PARTITION BY country, city), 4) AS rating_share,
        FIRST_VALUE(rating, TRUE) OVER (ORDER BY rating, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_rating,
        SUM(rating) OVER (PARTITION BY city ORDER BY rating, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_rating
    FROM suppliers
)
SELECT
    id,
    supplier_name,
    country,
    city,
    rating,
    rating_share
FROM enriched
ORDER BY country DESC NULLS FIRST
