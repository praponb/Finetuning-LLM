WITH filtered_customers AS (
    SELECT *, CAST(is_active AS INT) AS is_active_flag
    FROM customers
    WHERE is_active
),
country_summary AS (
    SELECT country, COUNT(CASE WHEN city = 'Bangkok' THEN 1 END) AS bangkok_count
    FROM filtered_customers
    GROUP BY country
)
SELECT country, bangkok_count
FROM country_summary
ORDER BY bangkok_count
