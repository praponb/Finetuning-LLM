WITH latest_match AS (
    SELECT
        s.*,
        x.quantity,
        x.unit_price
    FROM orders s
    JOIN LATERAL (
            SELECT c.quantity, c.unit_price
            FROM order_items c
            WHERE c.order_id = s.id
            ORDER BY c.quantity DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
)
SELECT
    id,
    store_id,
    status,
    channel,
    discount,
    shipping_fee,
    quantity,
    CASE WHEN status RLIKE '^(SHIPPED|PENDING)$' THEN 'CORE' ELSE 'PRIORITY' END AS status_zone
FROM latest_match
