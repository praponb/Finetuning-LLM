WITH valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        store_name,
        city,
        opened_date,
        LOWER(city) AS city_lower,
        CASE WHEN region = 'Central' THEN 1 ELSE 0 END AS is_central
    FROM stores
    WHERE sqft < 8853
      AND opened_date >= DATE '2023-07-06'
)
SELECT id, city
FROM valid
INTERSECT ALL
SELECT id, city
FROM stores
WHERE opened_date < '2025-10-20'
