WITH metrics AS (
    SELECT
        *,
        ROUND(rating / SUM(rating) OVER (PARTITION BY country), 4) AS rating_share,
        ROUND(rating / SUM(rating) OVER (PARTITION BY country), 4) AS rating_share_2,
        AVG(rating) OVER (PARTITION BY country, city ORDER BY rating, id ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) AS rating_avg_5_prior,
        FIRST_VALUE(rating) IGNORE NULLS OVER (PARTITION BY country ORDER BY rating, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_rating
    FROM suppliers
)
SELECT
    id,
    supplier_name,
    country,
    rating_share,
    rating_share_2,
    rating_avg_5_prior,
    first_known_rating,
    CASE WHEN country RLIKE '^(UK|TH)$' THEN 'CORE' ELSE 'GROUP_B' END AS country_zone
FROM metrics
ORDER BY rating_share DESC NULLS LAST;
