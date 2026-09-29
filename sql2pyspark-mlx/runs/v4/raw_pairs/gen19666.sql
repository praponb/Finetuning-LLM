WITH eligible AS (
    SELECT s.*
    FROM stores s
    WHERE EXISTS (
            SELECT 1
            FROM inventory c
            WHERE c.store_id = s.id
              AND c.last_restock <= s.opened_date
        )
)
SELECT
    id,
    store_name,
    city,
    sqft,
    CASE WHEN store_name RLIKE '^(Mall Central|Old Town)$' THEN 'OTHER_ZONE' ELSE 'GROUP_A' END AS store_name_zone
FROM eligible
ORDER BY id;
