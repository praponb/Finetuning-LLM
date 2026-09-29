WITH with_dims AS (
    -- Attach stores attributes
    SELECT
        a.*,
        b.sqft,
        b.store_name,
        b.region
    FROM inventory a
    LEFT JOIN stores b
        ON a.store_id = b.id
)
SELECT
    id,
    store_id,
    product_id,
    quantity,
    sqft,
    store_name,
    CASE WHEN store_name RLIKE '^(Airport Kiosk|Riverside)$' THEN 'GROUP_A' WHEN store_name RLIKE '^(Siam Flagship|Downtown Express)$' THEN 'EXTENDED' ELSE 'PRIORITY' END AS store_name_zone
FROM with_dims
ORDER BY store_name_zone, quantity DESC;
