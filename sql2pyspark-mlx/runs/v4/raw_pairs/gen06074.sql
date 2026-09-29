WITH filtered AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        COALESCE(supplier_name, 'N/A') AS supplier_name_filled,
        COALESCE(city, 'N/A') AS city_clean
    FROM suppliers
    WHERE LENGTH(supplier_name) > 12
),
cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        CONCAT(supplier_name, ' - ', city) AS supplier_name_city,
        CASE WHEN rating >= 4.0 THEN 'High' WHEN rating >= 1.7 THEN 'Medium' ELSE 'Low' END AS rating_band
    FROM filtered
    WHERE city IS NOT NULL
      AND country IN ('US', 'TH', 'DE')
),
kept AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM cleaned s
    WHERE NOT EXISTS (
            SELECT 1
            FROM products c
            WHERE c.supplier_id = s.id
              AND c.price >= 520
        )
)
-- Native SQL PIVOT
SELECT *
FROM (
    SELECT
        country,
        city,
        rating
    FROM kept
) src
PIVOT (
    MAX(rating)
    FOR city IN ('Bangkok', 'Berlin', 'London')
);
