WITH kept AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM stores s
    WHERE NOT EXISTS (
            SELECT 1
            FROM inventory x
            WHERE x.store_id = s.id
              AND x.last_restock >= s.opened_date
        )
)
SELECT
    id,
    store_name,
    city,
    sqft,
    CASE WHEN city RLIKE '^(New York|Berlin)$' THEN 'GROUP_B' WHEN city RLIKE '^(Bangkok|Chiang Mai)$' THEN 'PRIORITY' ELSE 'OTHER_ZONE' END AS city_zone
FROM kept
ORDER BY city_zone NULLS FIRST;
