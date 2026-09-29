WITH eligible AS (
    SELECT s.*
    FROM orders s
    WHERE NOT EXISTS (
            SELECT 1
            FROM shipments x
            WHERE x.order_id = s.id
              AND x.delivered_date < s.order_date
        )
)
SELECT
    s.id,
    s.customer_id,
    s.store_id,
    s.status,
    s.channel,
    s.amount,
    s.discount,
    s.shipping_fee,
    (SELECT SUM(o2.amount) FROM orders o2 WHERE o2.customer_id = s.customer_id) AS sum_order_amount
FROM eligible s
ORDER BY discount DESC NULLS FIRST;
