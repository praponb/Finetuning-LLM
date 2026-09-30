WITH kept_keys AS (
    SELECT region
    FROM stores
    INTERSECT ALL
    SELECT region
    FROM stores
    WHERE region = 'Central'
),
reconciled AS (
    SELECT s.*
    FROM stores s
    INNER JOIN kept_keys k ON s.region = k.region
)
SELECT
    store_name,
    region,
    sqft,
    CASE WHEN region RLIKE '^(West|North)$' THEN 'PRIORITY' WHEN region RLIKE '^(South|East)$' THEN 'EXTENDED' ELSE 'GROUP_A' END AS region_zone
FROM reconciled;
