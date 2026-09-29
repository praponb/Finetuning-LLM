WITH not_flagged AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM orders s
    WHERE NOT EXISTS (
            SELECT 1
            FROM shipments ch
            WHERE ch.order_id = s.id
              AND ch.shipped_date <= s.order_date
        )
)
-- Aggregate with conditional metrics
SELECT
    channel,
    CORR(amount, discount) AS corr_amount_discount,
    STDDEV_SAMP(shipping_fee) AS shipping_fee_stddev,
    COUNT(*) FILTER (WHERE channel = 'store') AS store_rows
FROM not_flagged
GROUP BY channel
ORDER BY corr_amount_discount
