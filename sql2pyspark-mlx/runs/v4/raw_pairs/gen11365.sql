WITH valid AS (
    SELECT
        id,
        supplier_name,
        city,
        rating,
        CONCAT_WS('-', country, city) AS country_city_key,
        ROUND(rating * 0.85, 2) AS rating_adjusted,
        COALESCE(city, 'N/A') AS city_clean
    FROM suppliers
    WHERE city IS NOT NULL
)
SELECT
    CASE WHEN rating >= 4.4 THEN 'LARGE' ELSE 'SMALL' END AS rating_tier,
    COUNT_IF(city = 'Berlin') AS berlin_events,
    MIN(rating_adjusted) AS min_rating_adjusted
FROM valid
GROUP BY CASE WHEN rating >= 4.4 THEN 'LARGE' ELSE 'SMALL' END;
