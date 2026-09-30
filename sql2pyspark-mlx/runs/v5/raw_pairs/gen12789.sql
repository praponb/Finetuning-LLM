WITH base AS (
    SELECT *
    FROM suppliers
    WHERE country IN ('UK', 'DE') AND rating IS NULL AND country = 'TH'
),
suppliers_stats AS (
    SELECT
        country,
        city,
        SUM(CASE WHEN country = 'JP' THEN rating ELSE 0 END) AS jp_rating,
        SUM(CASE WHEN country = 'TH' THEN 1 ELSE 0 END) AS th_count
    FROM base
    GROUP BY country, city
)
SELECT
    country,
    city,
    jp_rating,
    th_count
FROM suppliers_stats
WHERE jp_rating > 70
ORDER BY country;
