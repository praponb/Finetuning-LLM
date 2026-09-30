WITH kept AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM suppliers s
    WHERE NOT EXISTS (
            SELECT 1
            FROM products c
            WHERE c.supplier_id = s.id
              AND c.supplier_id IN (10, 12, 25)
        )
)
SELECT
    id,
    supplier_name,
    city,
    rating,
    CASE WHEN city RLIKE '^(Tokyo|Singapore)$' THEN 'EXTENDED' WHEN city RLIKE '^(Bangkok|Chiang Mai)$' THEN 'GROUP_B' ELSE 'GROUP_A' END AS city_zone
FROM kept
ORDER BY city DESC;
