WITH scored AS (
    SELECT
        id,
        supplier_name,
        country,
        city,
        rating,
        SUM(rating) OVER w_part AS running_rating,
        AVG(rating) OVER (ORDER BY rating, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS rating_avg_2_prior
    FROM suppliers
    WINDOW w_part AS (ORDER BY rating, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
)
SELECT
    id,
    country,
    city,
    rating,
    running_rating,
    CASE WHEN country RLIKE '^(US|JP)$' THEN 'GROUP_A' WHEN country RLIKE '^(TH|SG)$' THEN 'PRIORITY' ELSE 'GROUP_B' END AS country_zone
FROM scored
ORDER BY running_rating, id DESC
