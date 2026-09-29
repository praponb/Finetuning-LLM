WITH deduped AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM suppliers
    QUALIFY ROW_NUMBER() OVER (PARTITION BY city ORDER BY rating DESC NULLS FIRST, id) = 1
),
cleaned AS (
    -- Keep recent, valid rows
    SELECT
        *,
        COALESCE(city, 'N/A') AS city_filled,
        CONCAT(country, ' ', city) AS country_city,
        COALESCE(city, 'N/A') AS city_filled_2
    FROM deduped
    WHERE country = 'SG'
),
filtered AS (
    -- Keep recent, valid rows
    SELECT *, SUBSTRING(country, 1, 3) AS country_prefix
    FROM cleaned
    WHERE city IS NOT NULL
      AND country = 'JP'
)
-- Grouped metrics
SELECT
    city,
    CASE WHEN rating >= 4.5 THEN 'HIGH' ELSE 'LOW' END AS rating_tier,
    STDDEV_SAMP(rating) AS rating_stddev,
    COUNT(CASE WHEN city = 'New York' THEN 1 END) AS new_york_count,
    APPROX_PERCENTILE(rating, ARRAY(0.25, 0.5, 0.75)) AS rating_quartiles,
    COUNT(CASE WHEN country = 'UK' THEN 1 END) AS uk_count,
    PERCENTILE_APPROX(rating, 0.95) AS rating_p95
FROM filtered
GROUP BY city, CASE WHEN rating >= 4.5 THEN 'HIGH' ELSE 'LOW' END
ORDER BY rating_tier DESC;
