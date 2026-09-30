WITH filtered AS (
    -- Keep recent, valid rows
    SELECT
        id,
        supplier_name,
        country,
        city,
        rating,
        COALESCE(supplier_name, 'Unknown') AS supplier_name_filled,
        COALESCE(city, 'N/A') AS resolved_city
    FROM suppliers
    WHERE supplier_name LIKE 'Sia%'
)
-- Grouped metrics
SELECT
    city,
    country,
    SUM(rating) AS total_rating,
    COUNT(DISTINCT country) AS distinct_countries,
    COUNT(CASE WHEN country = 'DE' THEN 1 END) AS de_count,
    DENSE_RANK() OVER (PARTITION BY city ORDER BY COUNT(DISTINCT country) DESC) AS distinct_countries_dense_rank
FROM filtered
GROUP BY city, country;
