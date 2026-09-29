WITH base AS (
    SELECT *, CAST(is_active AS INT) AS is_active_flag
    FROM customers
    WHERE NOT (country IN ('TH', 'US') AND email LIKE '%@startup.io')
),
customers_stats AS (
    SELECT country, COUNT(DISTINCT is_active_flag) AS unique_countrys, COUNT(CASE WHEN country = 'US' THEN 1 END) AS us_count
    FROM base
    GROUP BY country
)
SELECT *
FROM customers_stats
ORDER BY us_count DESC, country
LIMIT 5;
