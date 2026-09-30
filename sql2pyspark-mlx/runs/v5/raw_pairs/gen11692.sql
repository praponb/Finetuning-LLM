SELECT
    s.id,
    s.order_date,
    s.status,
    s.channel,
    s.shipping_fee,
    top1.shipped_date
FROM orders s
JOIN LATERAL (
        SELECT c.shipped_date
        FROM shipments c
        WHERE c.order_id = s.id
        ORDER BY c.cost DESC, c.id DESC
        LIMIT 1
    ) top1 ON TRUE
