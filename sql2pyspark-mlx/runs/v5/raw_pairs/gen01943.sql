WITH with_last AS (
    SELECT s.*, latest.quantity
    FROM stores s
    JOIN LATERAL (
            SELECT c.quantity
            FROM inventory c
            WHERE c.store_id = s.id
              AND c.last_restock <= s.opened_date
            ORDER BY c.last_restock DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
)
SELECT
    id,
    store_name,
    city,
    opened_date,
    sqft,
    quantity
FROM with_last
ORDER BY id DESC NULLS FIRST
