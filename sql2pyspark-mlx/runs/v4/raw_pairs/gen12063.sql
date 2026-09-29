WITH matched_keys AS (
    SELECT region, id
    FROM stores
    EXCEPT DISTINCT
    SELECT region, id
    FROM stores
    WHERE store_name LIKE 'Mal%'
),
kept_rows AS (
    SELECT s.*
    FROM stores s
    INNER JOIN matched_keys k ON s.region = k.region AND s.id = k.id
)
SELECT
    store_name,
    region,
    opened_date,
    sqft,
    CASE WHEN store_name RLIKE '^(Siam Flagship|Mall Central)$' THEN 'GROUP_B' WHEN store_name RLIKE '^(Downtown Express|Airport Kiosk)$' THEN 'GROUP_A' ELSE 'PRIORITY' END AS store_name_zone
FROM kept_rows
ORDER BY sqft DESC NULLS FIRST
