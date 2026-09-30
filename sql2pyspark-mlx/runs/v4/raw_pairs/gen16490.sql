WITH cleaned AS (
    -- Keep recent, valid rows
    SELECT *, LOWER(country) AS country_lower
    FROM suppliers
    WHERE supplier_name LIKE 'Eur%'
),
cleaned_suppliers AS (
    -- Keep recent, valid rows
    SELECT
        *,
        LENGTH(country) AS country_length,
        CONCAT(country, ', ', supplier_name) AS country_supplier_name
    FROM cleaned
    WHERE city IS NOT NULL
),
kept AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM cleaned_suppliers s
    WHERE NOT EXISTS (
            SELECT 1
            FROM products ch
            WHERE ch.supplier_id = s.id
              AND ch.launch_date BETWEEN '2023-07-26' AND '2024-07-25'
        )
)
-- Grouped metrics
SELECT
    country,
    STDDEV_SAMP(rating) AS rating_stddev,
    MAX(country_length) FILTER (WHERE country = 'JP') AS jp_max_country_length,
    SIZE(COLLECT_SET(city)) AS num_cities
FROM kept
GROUP BY country
ORDER BY rating_stddev DESC
