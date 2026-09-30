WITH with_last AS (
    SELECT
        s.id,
        s.customer_id,
        s.status,
        s.channel,
        latest.delivered_date
    FROM orders s
    LEFT JOIN LATERAL (
            SELECT c.delivered_date
            FROM shipments c
            WHERE c.order_id = s.id
            ORDER BY c.cost DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
)
SELECT
    id,
    channel,
    delivered_date
FROM with_last;
