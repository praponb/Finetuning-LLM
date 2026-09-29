WITH with_last AS (
    SELECT
        s.id,
        s.store_name,
        s.city,
        s.region,
        s.opened_date,
        s.sqft,
        x.quantity
    FROM stores s
    LEFT JOIN LATERAL (
            SELECT c.quantity
            FROM inventory c
            WHERE c.store_id = s.id
            ORDER BY c.last_restock DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
)
SELECT
    id,
    city,
    region,
    quantity,
    CASE WHEN region RLIKE '^(West|North)$' THEN 'EXTENDED' WHEN region RLIKE '^(South|East)$' THEN 'GROUP_A' ELSE 'PRIORITY' END AS region_zone
FROM with_last;
