SELECT
    s.id,
    s.store_name,
    s.city,
    s.opened_date,
    s.sqft,
    latest.status
FROM stores s
JOIN LATERAL (
        SELECT c.status
        FROM orders c
        WHERE c.store_id = s.id
        ORDER BY c.shipping_fee DESC, c.id DESC
        LIMIT 1
    ) latest ON TRUE
