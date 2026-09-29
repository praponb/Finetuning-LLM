WITH kept AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM orders s
    WHERE EXISTS (
            SELECT 1
            FROM payments ch
            WHERE ch.order_id = s.id
              AND ch.paid_at <= s.order_date
        )
)
SELECT
    id,
    customer_id,
    store_id,
    order_date,
    channel,
    amount,
    discount,
    shipping_fee
FROM kept
ORDER BY amount DESC NULLS LAST, channel DESC;
