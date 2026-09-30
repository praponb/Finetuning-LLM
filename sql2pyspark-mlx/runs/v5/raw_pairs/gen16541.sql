WITH valid AS (
    -- Keep recent, valid rows
    SELECT *, SUBSTRING(city, 1, 2) AS city_prefix
    FROM suppliers
    WHERE country NOT IN ('US', 'UK')
)
-- Aggregate with conditional metrics
SELECT
    city,
    CASE WHEN rating >= 4.3 THEN 'HIGH' ELSE 'LOW' END AS rating_tier,
    COUNT(DISTINCT country) AS distinct_countries,
    COUNT(CASE WHEN city = 'New York' THEN 1 END) AS new_york_count,
    PERCENTILE_APPROX(rating, 0.9) AS rating_p90,
    COUNT(*) AS num_rows,
    STDDEV_SAMP(rating) AS rating_stddev,
    SUM(CASE WHEN country = 'UK' THEN rating ELSE 0 END) AS uk_rating
FROM valid
GROUP BY city, CASE WHEN rating >= 4.3 THEN 'HIGH' ELSE 'LOW' END
