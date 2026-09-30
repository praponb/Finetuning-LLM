WITH scored AS (
    -- Ranking and gap features
    SELECT
        id,
        supplier_name,
        country,
        city,
        rating,
        AVG(rating) OVER w_running AS rating_avg_5,
        COUNT(rating) OVER w_part AS country_count_rating
    FROM suppliers
    WINDOW w_running AS (PARTITION BY city ORDER BY rating ASC, id ASC ROWS BETWEEN 5 PRECEDING AND CURRENT ROW), w_part AS (PARTITION BY country, city)
)
-- Multi-level aggregation
SELECT
    CASE WHEN country_count_rating >= 16 THEN 'HIGH' ELSE 'LOW' END AS country_count_rating_tier,
    city,
    COUNT(CASE WHEN country = 'TH' THEN 1 END) AS th_count,
    APPROX_PERCENTILE(rating, 0.9) AS rating_p90,
    MAX(country_count_rating) AS max_country_count_rating
FROM scored
GROUP BY country_count_rating_tier, city
