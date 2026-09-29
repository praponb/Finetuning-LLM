WITH with_latest AS (
    SELECT
        s.id,
        s.store_name,
        s.city,
        latest.amount
    FROM stores s
    LEFT JOIN LATERAL (
            SELECT c.amount
            FROM orders c
            WHERE c.store_id = s.id
            ORDER BY c.shipping_fee DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
)
SELECT
    id,
    city,
    amount
FROM with_latest
ORDER BY city DESC
