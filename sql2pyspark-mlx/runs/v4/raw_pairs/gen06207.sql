WITH matched_keys AS (
    -- Set difference on keys
    SELECT id, city
    FROM stores
    EXCEPT ALL
    SELECT id, city
    FROM stores
    WHERE opened_date BETWEEN '2023-04-02' AND '2024-02-01'
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM stores s
    INNER JOIN matched_keys k ON s.id = k.id AND s.city = k.city
)
SELECT
    id,
    city,
    region,
    opened_date,
    sqft,
    CASE WHEN region RLIKE '^(Central|South)$' THEN 'PRIORITY' WHEN region RLIKE '^(North|East)$' THEN 'GROUP_B' ELSE 'CORE' END AS region_zone
FROM verified
ORDER BY region DESC NULLS FIRST, sqft DESC
